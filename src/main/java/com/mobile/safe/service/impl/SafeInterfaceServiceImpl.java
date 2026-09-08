package com.mobile.safe.service.impl;

import com.alibaba.fastjson.JSON;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.mobile.safe.config.CommonConfig;
import com.mobile.safe.db.ClassifyBinaryDo;
import com.mobile.safe.db.ClassifyMultiDo;
import com.mobile.safe.db.ExtractInfoDo;
import com.mobile.safe.db.ModelsDo;
import com.mobile.safe.dto.CommonDto;
import com.mobile.safe.service.SafeInterfaceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
@Service
@Slf4j
public class SafeInterfaceServiceImpl implements SafeInterfaceService {

    private static final int MAX_429_RETRIES = 3;
    private static final long RETRY_429_INTERVAL_MS = 500L;
    private final AtomicLong classifyBinaryAlarmCount = new AtomicLong();
    private final AtomicLong classifyMultiAlarmCount = new AtomicLong();
    private final AtomicLong extractInfoAlarmCount = new AtomicLong();
    private final AtomicLong classifyBinaryRetryCount = new AtomicLong();
    private final AtomicLong classifyMultiRetryCount = new AtomicLong();
    private final AtomicLong extractInfoRetryCount = new AtomicLong();
    private final AtomicLong classifyBinarySuccessCount = new AtomicLong();
    private final AtomicLong classifyMultiSuccessCount = new AtomicLong();
    private final AtomicLong extractInfoSuccessCount = new AtomicLong();

    @Autowired
    private CommonConfig commonConfig;

    @Override
    public ModelsDo models() {
        RestTemplate restTemplate = commonConfig.getRestTemplate();

        ResponseEntity<String> response = restTemplate.getForEntity("", String.class);
        if (response.getStatusCodeValue() == 200) {
            String res = response.getBody();
            ModelsDo modelsDo = (ModelsDo) JSON.parseObject(res,ModelsDo.class);
            log.info("models{}",modelsDo);
            return modelsDo;
        }else {
            log.error("接口调用异常");
            return null;
        }
    }

    @Override
    public ClassifyBinaryDo classifyBinary(CommonDto dto) {
        classifyBinaryAlarmCount.incrementAndGet();
        RestTemplate restTemplate = commonConfig.getRestTemplate();
        // 构建请求体
        Map<String, Object> body = new HashMap<>();;
        body.put("content",dto.getContent());
        body.put("adapter",dto.getAdapter());

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(body);
        long startTime = System.currentTimeMillis();
        ResponseEntity<String> response = postWith429Retry(restTemplate,
                "http://188.106.25.14:9091/v1/classifyBinary", request, "classifyBinary");
        long endTime = System.currentTimeMillis();
//        log.info("classifyBinary请求开始{},{}",startTime,endTime);
        long duration = endTime - startTime;
        if (response == null) {
            return null;
        } else if (response.getStatusCodeValue() == 200) {
            String res = response.getBody();
            ClassifyBinaryDo classifyBinaryDo = (ClassifyBinaryDo) JSON.parseObject(res,ClassifyBinaryDo.class);
            classifyBinaryDo.setUse_time(duration);
            //log.info("classifyBinary结果{}",classifyBinaryDo);
            return classifyBinaryDo;
        } else {
            log.error("接口调用异常");
            return null;
        }
    }

    @Override
    public ClassifyMultiDo classifyMulti(CommonDto dto) {
        classifyMultiAlarmCount.incrementAndGet();
        RestTemplate restTemplate = commonConfig.getRestTemplate();
        // 构建请求体
        Map<String, Object> body = new HashMap<>();;
        body.put("content",dto.getContent());
        body.put("adapter",dto.getAdapter());

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(body);
        long startTime = System.currentTimeMillis();
        ResponseEntity<String> response = postWith429Retry(restTemplate,
                "http://188.106.25.14:9091/v1/classifyMulti", request, "classifyMulti");
        long endTime = System.currentTimeMillis();
//        log.info("classifyMulti请求开始{},{}",startTime,endTime);
        long duration = endTime - startTime;
        if (response == null) {
            return null;
        } else if (response.getStatusCodeValue() == 200) {
            String res = response.getBody();
            ClassifyMultiDo classifyMultiDo = (ClassifyMultiDo) JSON.parseObject(res,ClassifyMultiDo.class);
            classifyMultiDo.setUse_time(duration);
            //log.info("classifyMulti{}",classifyMultiDo);
            return classifyMultiDo;
        }else {
            log.error("接口调用异常");
            return null;
        }
    }

    @Override
    public ExtractInfoDo extractInfo(CommonDto dto) {

        extractInfoAlarmCount.incrementAndGet();

        RestTemplate restTemplate = commonConfig.getRestTemplate();
        // 构建请求体
        Map<String, Object> body = new HashMap<>();;
        body.put("content",dto.getContent());
        body.put("adapter",dto.getAdapter());

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(body);
        long startTime = System.currentTimeMillis();
        ResponseEntity<String> response = postWith429Retry(restTemplate,
                "http://188.106.25.14:9091/v1/extractInfo", request, "extractInfo");
        long endTime = System.currentTimeMillis();
//        log.info("extractInfo请求开始{},{}",startTime,endTime);
        long duration = endTime - startTime;
        if (response == null) {
            return null;
        } else if (response.getStatusCodeValue() == 200) {
            String res = response.getBody();
            ExtractInfoDo extractInfoDo = (ExtractInfoDo) JSON.parseObject(res,ExtractInfoDo.class);
            extractInfoDo.setUse_time(duration);
           // log.info("extractInfo{}",extractInfoDo);
            return extractInfoDo;
        }else {
            log.error("接口调用异常");
            return null;
        }
    }

    /**
     * RestTemplate 默认会把 429 转换成 HttpClientErrorException，因此必须在异常分支重试，
     * 不能只检查 ResponseEntity 的状态码。
     */
    private ResponseEntity<String> postWith429Retry(RestTemplate restTemplate,
                                                     String url,
                                                     HttpEntity<Map<String, Object>> request,
                                                     String apiName) {
        for (int retryCount = 0; retryCount <= MAX_429_RETRIES; retryCount++) {
            try {
                ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);
                if (response.getStatusCodeValue() == 200) {
                    incrementSuccessCount(apiName);
                }
                return response;
            } catch (HttpClientErrorException e) {
                if (e.getStatusCode() != HttpStatus.TOO_MANY_REQUESTS) {
                    throw e;
                }

                if (retryCount == MAX_429_RETRIES) {
                    log.error("{}接口持续返回429，已重试{}次，停止请求", apiName, MAX_429_RETRIES);
                    return null;
                }

                incrementRetryCount(apiName);
                log.warn("{}接口返回429，0.5秒后进行第{}次重试", apiName, retryCount + 1);
                try {
                    Thread.sleep(RETRY_429_INTERVAL_MS);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    log.warn("{}接口等待429重试时线程被中断", apiName);
                    return null;
                }
            }
        }
        return null;
    }

    private void incrementRetryCount(String apiName) {
        if ("classifyBinary".equals(apiName)) {
            classifyBinaryRetryCount.incrementAndGet();
        } else if ("classifyMulti".equals(apiName)) {
            classifyMultiRetryCount.incrementAndGet();
        } else if ("extractInfo".equals(apiName)) {
            extractInfoRetryCount.incrementAndGet();
        }
    }

    private void incrementSuccessCount(String apiName) {
        if ("classifyBinary".equals(apiName)) {
            classifyBinarySuccessCount.incrementAndGet();
        } else if ("classifyMulti".equals(apiName)) {
            classifyMultiSuccessCount.incrementAndGet();
        } else if ("extractInfo".equals(apiName)) {
            extractInfoSuccessCount.incrementAndGet();
        }
    }

    /**
     * 每分钟输出一次实际接口流量。告警数不包含429重试；HTTP请求总数包含重试。
     */
    @Scheduled(initialDelay = 60000L, fixedRate = 60000L)
    public void logOneMinuteApiStatistics() {
        long binaryAlarms = classifyBinaryAlarmCount.getAndSet(0);
        long multiAlarms = classifyMultiAlarmCount.getAndSet(0);
        long extractAlarms = extractInfoAlarmCount.getAndSet(0);
        long binaryRetries = classifyBinaryRetryCount.getAndSet(0);
        long multiRetries = classifyMultiRetryCount.getAndSet(0);
        long extractRetries = extractInfoRetryCount.getAndSet(0);
        long binarySuccesses = classifyBinarySuccessCount.getAndSet(0);
        long multiSuccesses = classifyMultiSuccessCount.getAndSet(0);
        long extractSuccesses = extractInfoSuccessCount.getAndSet(0);

        long apiCalls = binaryAlarms + multiAlarms + extractAlarms;
        long retries = binaryRetries + multiRetries + extractRetries;
        long successes = binarySuccesses + multiSuccesses + extractSuccesses;
        log.info("最近1分钟接口发送统计: 进入处理链告警数={}, 接口调用数={} " +
                        "[classifyBinary={}, classifyMulti={}, extractInfo={}], " +
                        "429重试请求数={} [classifyBinary={}, classifyMulti={}, extractInfo={}], " +
                        "HTTP请求总数={}, 成功告警请求总数={} " +
                        "[classifyBinary={}, classifyMulti={}, extractInfo={}], 第一阶段请求成功告警数={}",
                binaryAlarms, apiCalls, binaryAlarms, multiAlarms, extractAlarms,
                retries, binaryRetries, multiRetries, extractRetries, apiCalls + retries,
                successes,
                binarySuccesses, multiSuccesses, extractSuccesses, binarySuccesses);
    }
}
