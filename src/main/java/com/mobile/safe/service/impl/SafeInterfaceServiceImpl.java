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
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;
@Service
@Slf4j
public class SafeInterfaceServiceImpl implements SafeInterfaceService {

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
        RestTemplate restTemplate = commonConfig.getRestTemplate();
        // 构建请求体
        Map<String, Object> body = new HashMap<>();;
        body.put("content",dto.getContent());
        body.put("adapter",dto.getAdapter());

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(body);
        long startTime = System.currentTimeMillis();
        ResponseEntity<String> response = restTemplate.postForEntity("http://188.106.25.14:9091/v1/classifyBinary",request,String.class);
        long endTime = System.currentTimeMillis();
//        log.info("classifyBinary请求开始{},{}",startTime,endTime);
        long duration = endTime - startTime;
        if (response.getStatusCodeValue() == 200) {
            String res = response.getBody();
            ClassifyBinaryDo classifyBinaryDo = (ClassifyBinaryDo) JSON.parseObject(res,ClassifyBinaryDo.class);
            classifyBinaryDo.setUse_time(duration);
            //log.info("classifyBinary结果{}",classifyBinaryDo);
            return classifyBinaryDo;
        }else if(response.getStatusCodeValue() == 429){
            log.error("classifyBinary接口调用失败，返回429");
            return null;
        } else {
            log.error("接口调用异常");
            return null;
        }
    }

    @Override
    public ClassifyMultiDo classifyMulti(CommonDto dto) {
        RestTemplate restTemplate = commonConfig.getRestTemplate();
        // 构建请求体
        Map<String, Object> body = new HashMap<>();;
        body.put("content",dto.getContent());
        body.put("adapter",dto.getAdapter());

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(body);
        long startTime = System.currentTimeMillis();
        ResponseEntity<String> response = restTemplate.postForEntity("http://188.106.25.14:9091/v1/classifyMulti",request,String.class);
        long endTime = System.currentTimeMillis();
//        log.info("classifyMulti请求开始{},{}",startTime,endTime);
        long duration = endTime - startTime;
        if (response.getStatusCodeValue() == 200) {
            String res = response.getBody();
            ClassifyMultiDo classifyMultiDo = (ClassifyMultiDo) JSON.parseObject(res,ClassifyMultiDo.class);
            classifyMultiDo.setUse_time(duration);
            //log.info("classifyMulti{}",classifyMultiDo);
            return classifyMultiDo;
        }else if(response.getStatusCodeValue() == 429){
            log.error("classifyMulti接口调用失败，返回429");
            return null;
        }else {
            log.error("接口调用异常");
            return null;
        }
    }

    @Override
    public ExtractInfoDo extractInfo(CommonDto dto) {

        RestTemplate restTemplate = commonConfig.getRestTemplate();
        // 构建请求体
        Map<String, Object> body = new HashMap<>();;
        body.put("content",dto.getContent());
        body.put("adapter",dto.getAdapter());

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(body);
        long startTime = System.currentTimeMillis();
        ResponseEntity<String> response = restTemplate.postForEntity("http://188.106.25.14:9091/v1/extractInfo",request,String.class);
        long endTime = System.currentTimeMillis();
//        log.info("extractInfo请求开始{},{}",startTime,endTime);
        long duration = endTime - startTime;
        if (response.getStatusCodeValue() == 200) {
            String res = response.getBody();
            ExtractInfoDo extractInfoDo = (ExtractInfoDo) JSON.parseObject(res,ExtractInfoDo.class);
            extractInfoDo.setUse_time(duration);
           // log.info("extractInfo{}",extractInfoDo);
            return extractInfoDo;
        }else if(response.getStatusCodeValue() == 429){
            log.error("extractInfo接口调用失败，返回429");
            return null;
        }else {
            log.error("接口调用异常");
            return null;
        }
    }
}
