package com.mobile.safe.task;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mobile.safe.common.ChatResponse;
import com.mobile.safe.db.AlarmRecordResult;
import com.mobile.safe.db.AlarmRecordResultDTO;
import com.mobile.safe.db.ClassifyBinaryDo;
import com.mobile.safe.db.ClassifyMultiDo;
import com.mobile.safe.db.ExtractInfoDo;
import com.mobile.safe.dto.CommonDto;
import com.mobile.safe.service.AlarmRecordResultService;
import com.mobile.safe.service.SafeInterfaceService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PreDestroy;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@Slf4j
public class KafkaConsumer {

    @Autowired
    private SafeInterfaceService safeInterfaceService;

    @Autowired
    private AlarmRecordResultService alarmRecordResultService;

    public static final String TOPIC_TEST = "asap_superset";
    private static final String ISOP_DEVICE_TYPE = "Nsfocus.ISOP";

    private static final Set<String> ALLOWED_DEVICE_TYPES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(
                    "DBApp_AT", "H3C_IDS", "Nsfocus_IDS", "Nsfocus_FLOW",
                    "Nsfocus_FLOW_5G", "Dptech_WAF", "Nsfocus_FLOW_NEW",
                    "Nsfocus_FLOW_DPI", "Nsfocus.ISOP"
            ))
    );

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("Asia/Shanghai"));


    private static final int API_CONCURRENCY = 128;
    private static final int API_QUEUE_CAPACITY = 1024;
    private final Object metricsLock = new Object();
    private int metricsCompletedCount;
    private long metricsEarliestStartNanos;
    private long metricsTotalTaskNanos;
    private long metricsMaxTaskNanos;

    /**
     * 所有 Kafka listener 共享同一个执行器，使不同 poll 批次中筛选出的告警可以一起填满工作线程。
     *
     * core/max 使用相同值，确保实际并发稳定在 API_CONCURRENCY。
     * 队列满时阻塞提交线程形成背压，避免 CallerRunsPolicy 让 8 个 Kafka listener 额外调用接口，
     * 从而突破设定的并发上限。
     */
    private final ThreadPoolExecutor msgExecutor = new ThreadPoolExecutor(
            API_CONCURRENCY, API_CONCURRENCY, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(API_QUEUE_CAPACITY),
            new ThreadFactory() {
                private final AtomicInteger counter = new AtomicInteger(0);
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "kafka-msg-parallel-" + counter.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                }
            },
            (task, executor) -> {
                if (executor.isShutdown()) {
                    throw new RejectedExecutionException("告警接口执行器已关闭");
                }
                try {
                    executor.getQueue().put(task);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RejectedExecutionException("等待告警接口队列时被中断", e);
                }
            }
    );

    {
        msgExecutor.prestartAllCoreThreads();
    }

    @PreDestroy
    public void shutdownMsgExecutor() {
        msgExecutor.shutdown();
        try {
            if (!msgExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("应用关闭时告警队列仍未清空: waiting={}, active={}",
                        msgExecutor.getQueue().size(), msgExecutor.getActiveCount());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("等待告警队列关闭时被中断: waiting={}, active={}",
                    msgExecutor.getQueue().size(), msgExecutor.getActiveCount());
        }
    }


    private final RestTemplate restTemplate = new RestTemplate();
    {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(5000);
        factory.setReadTimeout(30000);
        restTemplate.setRequestFactory(factory);
    }

    private final ObjectMapper objectMapper = new ObjectMapper();


    @KafkaListener(topics = TOPIC_TEST, concurrency = "8")
    public void topic_alarm(List<String> messages, Acknowledgment ack) {
        if (CollUtil.isEmpty(messages)) {
            ack.acknowledge();
            return;
        }

        log.info("获取到kafka消息条数:{}, 线程:{}", messages.size(), Thread.currentThread().getName());

        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicInteger queuedCount = new AtomicInteger(0);

        try {
            //  第一条消息 处理 80/20 =====
//            processFirstMessageSafely(messages.get(0));

            // 先过滤再入共享队列，避免不符合条件的消息占用接口工作线程。
            List<AlarmRecordResultDTO> qualifiedAlarms = new ArrayList<>();
            for (String msg : messages) {
                try {
                    AlarmRecordResultDTO alarm = parseQualifiedAlarm(msg);
                    if (alarm != null) {
                        qualifiedAlarms.add(alarm);
                    }
                } catch (Throwable t) {
                    errorCount.incrementAndGet();
                    log.error("单条消息解析异常, raw={}", msg, t);
                }
            }

            log.info("当前 Kafka 批次符合接口调用条件的告警数:{}, 共享队列等待数:{}, 接口处理中:{}",
                    qualifiedAlarms.size(), msgExecutor.getQueue().size(), msgExecutor.getActiveCount());

            // 只负责入队，不在 Kafka listener 中等待接口完成。这样后续 poll 的小批次可以继续
            // 补充共享队列；只要全局有足够积压，worker 就会持续满负荷运行。
            for (AlarmRecordResultDTO alarm : qualifiedAlarms) {
                msgExecutor.execute(() -> {
                    long taskStartNanos = System.nanoTime();
                    try {
                        AlarmRecordResult r = processQualifiedAlarm(alarm);
                        if (r != null) {
                            alarmRecordResultService.save(r);
                        }
                    } catch (Throwable t) {
                        log.error("单条告警接口处理异常, srcIp={}, dstIp={}",
                                alarm.getSrcIp(), alarm.getDstIp(), t);
                    } finally {
                        recordTaskMetrics(taskStartNanos, System.nanoTime());
                    }
                });
                queuedCount.incrementAndGet();
            }

            log.info("Kafka 批次入队完成: 原始消息数={}, 符合条件数={}, 入队数={}, 解析异常={}, " +
                            "共享队列等待数={}, 接口处理中={}",
                    messages.size(), qualifiedAlarms.size(), queuedCount.get(), errorCount.get(),
                    msgExecutor.getQueue().size(), msgExecutor.getActiveCount());
        } catch (Exception e) {
            log.error("消费数据入队故障: 已入队={}, 原始消息数={}", queuedCount.get(), messages.size(), e);
        } finally {
            // ACK 表示本批消息已进入进程内队列；接口失败仍沿用原逻辑记录日志并跳过。
            ack.acknowledge();
        }
    }

    /**
     * 每完成一个并发窗口的告警处理链输出一次吞吐统计。任务耗时包含接口调用和最终入库；
     * 窗口耗时从其中最早开始的任务算到最后一条完成。
     */
    private void recordTaskMetrics(long taskStartNanos, long taskEndNanos) {
        synchronized (metricsLock) {
            if (metricsCompletedCount == 0 || taskStartNanos < metricsEarliestStartNanos) {
                metricsEarliestStartNanos = taskStartNanos;
            }

            long taskNanos = taskEndNanos - taskStartNanos;
            metricsTotalTaskNanos += taskNanos;
            metricsMaxTaskNanos = Math.max(metricsMaxTaskNanos, taskNanos);
            metricsCompletedCount++;

            if (metricsCompletedCount == API_CONCURRENCY) {
                double windowSeconds = (taskEndNanos - metricsEarliestStartNanos) / 1_000_000_000.0;
                double averageMillis = metricsTotalTaskNanos / (double) API_CONCURRENCY / 1_000_000.0;
                double maxMillis = metricsMaxTaskNanos / 1_000_000.0;
                double throughput = windowSeconds > 0 ? API_CONCURRENCY / windowSeconds : 0;

                log.info("{}条告警处理完成: 窗口耗时={}s, 单条平均耗时={}ms, 单条最大耗时={}ms, " +
                                "吞吐量={}条/s, 共享队列等待数={}, 接口处理中={}",
                        API_CONCURRENCY,
                        String.format(Locale.ROOT, "%.3f", windowSeconds),
                        String.format(Locale.ROOT, "%.1f", averageMillis),
                        String.format(Locale.ROOT, "%.1f", maxMillis),
                        String.format(Locale.ROOT, "%.2f", throughput),
                        msgExecutor.getQueue().size(), msgExecutor.getActiveCount());

                metricsCompletedCount = 0;
                metricsEarliestStartNanos = 0;
                metricsTotalTaskNanos = 0;
                metricsMaxTaskNanos = 0;
            }
        }
    }


    private void processFirstMessageSafely(String firstMsg) {
        try {
            AlarmRecordResultDTO alarm = JSON.parseObject(firstMsg, AlarmRecordResultDTO.class);
            if (alarm == null) return;

            String alarmTimeStr = alarm.getCreateTime() != null ? DATE_FMT.format(alarm.getCreateTime().toInstant()) : "null";
            log.info("kafka 当前批次第一个告警时间:{} deviceType:{} appProtocol:{}", alarmTimeStr, StringUtils.defaultString(alarm.getDeviceType()), StringUtils.defaultString(alarm.getAppProtocol()));

            processFirstMessage(alarm);
        } catch (Exception e) {
            log.error("第一条消息处理失败, raw={}", firstMsg, e);
        }
    }

    /**
     * 第一条 80/20 逻辑
     */
    private void processFirstMessage(AlarmRecordResultDTO alarmRecordResultDTO) {
        if (ThreadLocalRandom.current().nextInt(100) < 80) {
            log.info("第一个 进入80%概率 走原有流程 =======================");
            String contentOne = resolveContent(alarmRecordResultDTO);
            if (StringUtils.isBlank(contentOne)) {
                log.warn("第一条告警content为空，跳过");
                return;
            }
            AlarmRecordResult result = getAlarmRecordResult(contentOne, alarmRecordResultDTO);
            if (result != null) {
                alarmRecordResultService.save(result);
                log.info("第一个 原有流程入库成功");
            }
        } else {
            log.info("第一个 进入20%概率 走大模型判断 =======================");
            processLlmFlow(alarmRecordResultDTO);
        }
    }

    /**
     * 20% 大模型判断
     */
    private void processLlmFlow(AlarmRecordResultDTO alarmRecordResultDTO) {
        try {
            String payloadStr = StringUtils.defaultString(alarmRecordResultDTO.getPayload());

            String contentTemplate = "SYSTEM_INSTRUCTION = \"\"\"Act as a cybersecurity analyst who is skilled in identifying and assessing potential threats in textual data.\n" +
                    "Please analyze the following request payload to determine if it represents a web attack.Respond only with 'Yes' if it is a web attack, or 'No' if it is not.\n" +
                    "Pay attention to some edge cases\n" +
                    "1. If a login, authentication, admin, or root endpoint carries a non‑empty\n" +
                    "plaintext password, especially admin/root with a weak password, classify the\n" +
                    "request as Yes.\n" +
                    "2. Treat the following image‑thumbnail requests as No when they only reference\n" +
                    "a normal image file and contain no other explicit attack indicators:\n" +
                    "- /images.php with filename=../pic/...<image> and numeric width/height\n" +
                    "- /include/thumb.php with dir=../upload/...<image> and numeric x/y\n" +
                    "A .php endpoint or a relative path such as ../pic/ or ../upload/ is not by\n" +
                    "itself sufficient evidence of an attack.\n" +
                    "Here is the given payload of the request:\n" +
                    "{}\n" +
                    "\"\"\"";

            String contentText = contentTemplate.replace("{}", payloadStr);

            Map<String, Object> message = new LinkedHashMap<>();
            message.put("role", "user");
            message.put("content", contentText);

            Map<String, Object> requestBodyMap = new LinkedHashMap<>();
            requestBodyMap.put("model", "O/Qwen3.6-27B");
            requestBodyMap.put("messages", Collections.singletonList(message));
            requestBodyMap.put("stream", false);

            String requestBody = objectMapper.writeValueAsString(requestBodyMap);

            String url = "http://188.103.147.179:30175/gateway/api/1hYY83";
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set("Authorization-Gateway", "sk-77ba3eda-fa0c-4185-9ecd-2652a06c2cef");

            HttpEntity<String> httpEntity = new HttpEntity<>(requestBody, headers);
            ResponseEntity<String> responseEntity = restTemplate.exchange(url, HttpMethod.POST, httpEntity, String.class);

            ChatResponse chatResponse = objectMapper.readValue(responseEntity.getBody(), ChatResponse.class);

            String content = "";
            if (chatResponse.getChoices() != null && !chatResponse.getChoices().isEmpty()) {
                content = StringUtils.defaultString(chatResponse.getChoices().get(0).getMessage().getContent());
            }
            log.info("大模型调用 AI回复: {}", content);

            if ("Yes".equalsIgnoreCase(content.trim())) {
                log.info("大模型调用返回Yes，继续调用三方接口");
                CommonDto commonDto = new CommonDto();
                commonDto.setContent(Collections.singletonList(payloadStr));
                ai(alarmRecordResultDTO, commonDto);
            } else if ("No".equalsIgnoreCase(content.trim())) {
                log.info("大模型调用返回No");
                AlarmRecordResult alarmRecordResult = new AlarmRecordResult();
                BeanUtil.copyProperties(alarmRecordResultDTO, alarmRecordResult);
                alarmRecordResult.setAlarmTime(alarmRecordResultDTO.getCreateTime());
                alarmRecordResultService.save(alarmRecordResult);

            } else {
                log.warn("大模型返回非预期内容:'{}'，跳过", content);
            }
        } catch (Exception e) {
            log.error("第一条大模型流程失败", e);
        }
    }

    /**
     * 大模型判定Yes后的三方接口调用  入库
     */
    private void ai(AlarmRecordResultDTO alarmRecordResultDTO, CommonDto commonDto) {
        commonDto.setAdapter("multi_cate");
        ClassifyMultiDo multiResult = safeInterfaceService.classifyMulti(commonDto);
        if (multiResult != null) {
            alarmRecordResultDTO.setClassifyMultiUseTime(multiResult.getUse_time());
            if (multiResult.getUsage() != null) {
                alarmRecordResultDTO.setClassifyMultiPromptTokens(multiResult.getUsage().getPrompt_tokens());
                alarmRecordResultDTO.setClassifyMultiCompletionTokens(multiResult.getUsage().getCompletion_tokens());
                alarmRecordResultDTO.setClassifyMultiTotalTokens(multiResult.getUsage().getTotal_tokens());
            }
        }
        if (multiResult == null || multiResult.getResponse() == null) {
            log.warn("safeInterfaceService.multiResult 异常:{}，跳过", commonDto);
            return;
        }
        String multiAnswer = Optional.ofNullable(multiResult)
                .map(ClassifyMultiDo::getResponse)
                .map(ClassifyMultiDo.Response::getResult)
                .orElse("");
        alarmRecordResultDTO.setAttackType(multiAnswer);

        commonDto.setAdapter("multi_cate");
        ExtractInfoDo extractResult = safeInterfaceService.extractInfo(commonDto);
        if (extractResult != null) {
            alarmRecordResultDTO.setExtractInfoUseTime(extractResult.getUse_time());
            if (extractResult.getUsage() != null) {
                alarmRecordResultDTO.setExtractInfoPromptTokens(extractResult.getUsage().getPrompt_tokens());
                alarmRecordResultDTO.setExtractInfoCompletionTokens(extractResult.getUsage().getCompletion_tokens());
                alarmRecordResultDTO.setExtractInfoTotalTokens(extractResult.getUsage().getTotal_tokens());
            }
        }
        if (extractResult == null || extractResult.getResponse() == null) {
            log.warn("safeInterfaceService.extractResult 异常:{}，跳过", commonDto);
            return;
        }
        String extractAnswer = Optional.ofNullable(extractResult)
                .map(ExtractInfoDo::getResponse)
                .map(ExtractInfoDo.ResponseData::getResult)
                .orElse("");
        alarmRecordResultDTO.setAttackField(extractAnswer);

        AlarmRecordResult alarmRecordResult = new AlarmRecordResult();
        BeanUtil.copyProperties(alarmRecordResultDTO, alarmRecordResult);
        alarmRecordResult.setAlarmTime(alarmRecordResultDTO.getCreateTime());
        alarmRecordResultService.save(alarmRecordResult);
        log.info("第一个 20%大模型流程 入库成功 =======================");
    }

    /**
     * 单条消息
     */
    private AlarmRecordResultDTO parseQualifiedAlarm(String msg) {
        AlarmRecordResultDTO alarm = JSON.parseObject(msg, AlarmRecordResultDTO.class);
        if (alarm == null) return null;

        String deviceType = StringUtils.defaultString(alarm.getDeviceType());
        String appProtocol = StringUtils.defaultString(alarm.getAppProtocol());

        boolean pass = ISOP_DEVICE_TYPE.equals(deviceType) || (ALLOWED_DEVICE_TYPES.contains(deviceType) && "HTTP".equals(appProtocol));
        if (!pass) return null;

        String content = resolveContent(alarm);
        if (StringUtils.isBlank(content)) return null;

        return alarm;
    }

    private AlarmRecordResult processQualifiedAlarm(AlarmRecordResultDTO alarm) {
        String content = resolveContent(alarm);
        return getAlarmRecordResult(content, alarm);
    }

    /**
     *
     *  外层已并行
     */
    private AlarmRecordResult getAlarmRecordResult(String content, AlarmRecordResultDTO alarm) {
        CommonDto commonDto = new CommonDto();
        commonDto.setContent(Collections.singletonList(content));
        commonDto.setAdapter("two_cate");

        ClassifyBinaryDo binaryResult = safeInterfaceService.classifyBinary(commonDto);
        if (binaryResult == null || binaryResult.getResponse() == null) {
            log.warn("safeInterfaceService.classifyBinary 异常:{}，跳过", commonDto);
            return null;
        }

        alarm.setClassifyBinaryUseTime(binaryResult.getUse_time());
        if (binaryResult.getUsage() != null) {
            alarm.setClassifyBinaryPromptTokens(binaryResult.getUsage().getPrompt_tokens());
            alarm.setClassifyBinaryCompletionTokens(binaryResult.getUsage().getCompletion_tokens());
            alarm.setClassifyBinaryTotalTokens(binaryResult.getUsage().getTotal_tokens());
        }

        String binaryAnswer = Optional.ofNullable(binaryResult)
                .map(ClassifyBinaryDo::getResponse)
                .map(ClassifyBinaryDo.Response::getResult)
                .orElse("");
        alarm.setIsSafeAttack(binaryAnswer);

        if ("Yes".equalsIgnoreCase(binaryAnswer)) {
            //  外层msgExecutor已并行
            commonDto.setAdapter("multi_cate");
            ClassifyMultiDo multiResult = safeInterfaceService.classifyMulti(commonDto);
            ExtractInfoDo extractResult = safeInterfaceService.extractInfo(commonDto);

            if (multiResult != null) {
                alarm.setClassifyMultiUseTime(multiResult.getUse_time());
                if (multiResult.getUsage() != null) {
                    alarm.setClassifyMultiPromptTokens(multiResult.getUsage().getPrompt_tokens());
                    alarm.setClassifyMultiCompletionTokens(multiResult.getUsage().getCompletion_tokens());
                    alarm.setClassifyMultiTotalTokens(multiResult.getUsage().getTotal_tokens());
                }
            }
            if (multiResult == null || multiResult.getResponse() == null) {
                log.warn("safeInterfaceService.multiResult 异常:{}，跳过", commonDto);
                return null;
            }
            String multiAnswer = Optional.ofNullable(multiResult)
                    .map(ClassifyMultiDo::getResponse)
                    .map(ClassifyMultiDo.Response::getResult)
                    .orElse("");
            alarm.setAttackType(multiAnswer);

            if (extractResult != null) {
                alarm.setExtractInfoUseTime(extractResult.getUse_time());
                if (extractResult.getUsage() != null) {
                    alarm.setExtractInfoPromptTokens(extractResult.getUsage().getPrompt_tokens());
                    alarm.setExtractInfoCompletionTokens(extractResult.getUsage().getCompletion_tokens());
                    alarm.setExtractInfoTotalTokens(extractResult.getUsage().getTotal_tokens());
                }
            }
            if (extractResult == null || extractResult.getResponse() == null) {
                log.warn("safeInterfaceService.extractResult 异常:{}，跳过", commonDto);
                return null;
            }
            String extractAnswer = Optional.ofNullable(extractResult)
                    .map(ExtractInfoDo::getResponse)
                    .map(ExtractInfoDo.ResponseData::getResult)
                    .orElse("");
            alarm.setAttackField(extractAnswer);
        }



        AlarmRecordResult alarmRecordResult = new AlarmRecordResult();
        BeanUtil.copyProperties(alarm, alarmRecordResult);
        alarmRecordResult.setAlarmTime(alarm.getCreateTime());
        alarmRecordResult.setPayload(content);
        return alarmRecordResult;
    }

    /**
     * 统一提取content字段（request_payload > request_message > payload）
     */
    private String resolveContent(AlarmRecordResultDTO dto) {
        return Optional.ofNullable(dto.getRequestPayload())
                .filter(s -> s != null && !s.trim().isEmpty())
                .orElseGet(() -> Optional.ofNullable(dto.getRequestMessage())
                        .filter(s -> s != null && !s.trim().isEmpty())
                        .orElse(dto.getPayload()));
    }
}
