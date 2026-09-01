package com.mobile.safe.task;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.io.resource.ResourceUtil;
import com.alibaba.fastjson.JSON;

import com.mobile.safe.dao.AlarmRecordResultDao;
import com.mobile.safe.db.*;
import com.mobile.safe.dto.CommonDto;
import com.mobile.safe.service.AlarmRecordResultService;
import com.mobile.safe.service.ResultService;
import com.mobile.safe.service.SafeInterfaceService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.text.SimpleDateFormat;
import java.util.*;

@Component
@Slf4j
public class KafkaConsumer {

    @Autowired
    private SafeInterfaceService safeInterfaceService;

    @Autowired
    private AlarmRecordResultService alarmRecordResultService;

    public static final String TOPIC_TEST = "asap_superset";


    @KafkaListener(topics = TOPIC_TEST, concurrency = "5")
    public void topic_alarm(List<String> messages, Acknowledgment ack) {

        // 测试
//        String test = ResourceUtil.readUtf8Str("test.json");
//        List<String> testList = JSON.parseArray(test, String.class);

        log.info("获取到kafka消息条数:{}, 线程:{}", messages.size(), Thread.currentThread().getName());

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        List<AlarmRecordResult> alarmRecordResultList = new ArrayList<>();
//        List<List<String>> partition = ListUtil.partition(messages, 30);
//        List<String> strings = partition.get(0);

        try {
            if (messages != null && !messages.isEmpty()) {
                for (String msg : messages) {

                    AlarmRecordResultDTO alarm;
                    try {
                        alarm = JSON.parseObject(msg, AlarmRecordResultDTO.class);
//                        log.info(" 转换 alarm 成功");
                    } catch (Exception e) {
                        log.error("JSON解析失败, raw={}", msg, e);
                        continue;
                    }
                    if (alarm == null) continue;

                    // request_payload -> request_message -> payload  字段优先级
                    String content = Optional.ofNullable(alarm.getRequestPayload())
                            .filter(s -> s != null && !s.trim().isEmpty())
                            .orElseGet(() -> Optional.ofNullable(alarm.getRequestMessage())
                                    .filter(s -> s != null && !s.trim().isEmpty())
                                    .orElse(alarm.getPayload()));

                    if (content == null || StringUtils.isBlank(content)) {
//                        log.warn("告警：content为空，跳过");
                        continue;
                    }

                    // -------------------------------------第一个调用--------------------------------------------
                    CommonDto commonDto = new CommonDto();
                    commonDto.setContent(Collections.singletonList(content));
                    commonDto.setAdapter("two_cate");
//                    log.info("safeInterfaceService.classifyBinary 调用开始: {}",JSON.toJSONString(commonDto));
                    ClassifyBinaryDo binaryResult = safeInterfaceService.classifyBinary(commonDto);

//                    log.info("safeInterfaceService.classifyBinary 结果: {}", JSON.toJSONString(binaryResult));


                    if (binaryResult == null || binaryResult.getResponse() == null) {
                        log.warn("safeInterfaceService.classifyBinary 异常 :{} ，跳过");
                        continue;
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

                    if (!"Yes".equalsIgnoreCase(binaryAnswer)) {
//                        log.info("safeInterfaceService.classifyBinary 结果: {}", binaryAnswer);
                        continue;
                    }

                    // -----------------------------------------第二个调用---------------------------------------------
                    commonDto.setAdapter("multi_cate");
//                    log.info("safeInterfaceService.classifyMulti 调用开始: {}",JSON.toJSONString(commonDto));
                    ClassifyMultiDo multiResult = safeInterfaceService.classifyMulti(commonDto);
//                    log.info("safeInterfaceService.classifyMulti 结果: {}", JSON.toJSONString(multiResult));
                    if (multiResult != null) {
                        alarm.setClassifyMultiUseTime(multiResult.getUse_time());
                        if (multiResult.getUsage() != null) {
                            alarm.setClassifyMultiPromptTokens(multiResult.getUsage().getPrompt_tokens());
                            alarm.setClassifyMultiCompletionTokens(multiResult.getUsage().getCompletion_tokens());
                            alarm.setClassifyMultiTotalTokens(multiResult.getUsage().getTotal_tokens());
                        }
                    }

                    String multiAnswer = Optional.ofNullable(multiResult)
                            .map(ClassifyMultiDo::getResponse)
                            .map(ClassifyMultiDo.Response::getResult)
                            .orElse("");

                    alarm.setAttackType(multiAnswer);

                    //---------------------------------------------第三个调用--------------------------------------------
                    commonDto.setAdapter("multi_cate");
//                    log.info("safeInterfaceService.extractInfo 调用开始: {}",JSON.toJSONString(commonDto));
                    ExtractInfoDo extractResult = safeInterfaceService.extractInfo(commonDto);
//                    log.info("safeInterfaceService.extractInfo 结果: {}", JSON.toJSONString(extractResult));

                    if (extractResult != null) {
                        alarm.setExtractInfoUseTime(extractResult.getUse_time());
                        if (extractResult.getUsage() != null) {
                            alarm.setExtractInfoPromptTokens(extractResult.getUsage().getPrompt_tokens());
                            alarm.setExtractInfoCompletionTokens(extractResult.getUsage().getCompletion_tokens());
                            alarm.setExtractInfoTotalTokens(extractResult.getUsage().getTotal_tokens());
                        }
                    }

                    String extractAnswer = Optional.ofNullable(extractResult)
                            .map(ExtractInfoDo::getResponse)
                            .map(ExtractInfoDo.ResponseData::getResult)
                            .orElse("");

                    alarm.setAttackField(extractAnswer);

                    //-------------------------------------------------构建alarmRecordResultList 准备入库-------------------------------------------

                    AlarmRecordResult alarmRecordResult = new AlarmRecordResult();
                    BeanUtil.copyProperties(alarm, alarmRecordResult);
                    alarmRecordResult.setAlarmTime(alarm.getCreateTime());

                    alarmRecordResultList.add(alarmRecordResult);

                }

                log.info("================ 开始 alarmRecordResult 入库 =============");

                alarmRecordResultService.saveAlarmData(alarmRecordResultList);

                log.info("告警数据插入完成 入库的大小：{}",alarmRecordResultList.size());

            }else{
                log.info("kafka 消息条数为空");
            }

        } catch (Exception e) {
            log.error("消费数据故障", e);
        } finally {
            ack.acknowledge();
        }
    }


}