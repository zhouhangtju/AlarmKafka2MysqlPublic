package com.mobile.safe.service.impl;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.mobile.safe.dao.AlarmData2Dao;
import com.mobile.safe.dao.AlarmRecordResult2Dao;
import com.mobile.safe.dao.AlarmRecordResultDao;
import com.mobile.safe.db.AlarmData2;
import com.mobile.safe.db.AlarmRecordResult;
import com.mobile.safe.db.AlarmRecordResult2;
import com.mobile.safe.service.AlarmRecordResultService;
import com.mobile.safe.service.ReadAlarmDataService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
@DS("db2")
public class ReadAlarmDataImpl extends ServiceImpl<AlarmData2Dao, AlarmData2> implements ReadAlarmDataService {

    @Autowired
    private AlarmData2Dao alarmData2Dao;
    @Autowired
    private AlarmRecordResultDao alarmRecordResultdao;
    @Autowired
    private AlarmRecordResult2Dao alarmRecordResult2dao;



    @Override
    public List<AlarmRecordResult> readAlarmData(Long startID,Long endID) {

        List<AlarmData2> data2List = getRecordsGreaterThanId(startID,endID);
        ArrayList<AlarmRecordResult> results = new ArrayList<>();
        if(data2List!=null){
            log.info("原始数据{}",data2List.get(0).getResponseMessage());
        }
        log.info("本次更新条数{}",data2List.size());
        data2List.forEach(item -> {
            AlarmRecordResult recordResult = new AlarmRecordResult();
            recordResult.setId(item.getID());
            recordResult.setAlarmName(item.getAlarmName());
            recordResult.setSrcIp(item.getSrcIp());
            recordResult.setDstIp(item.getDstIp());
            recordResult.setProtocol(item.getProtocol());
            if(StringUtils.isNotEmpty(item.getSrcPort())){
                recordResult.setAttackPort(Integer.valueOf(item.getSrcPort()));
            }
            recordResult.setAlarmTime(item.getLastAlarmTime());
            recordResult.setAttackField(item.getKeyword());
            recordResult.setPayload(item.getPayload());
            recordResult.setResponseCode(item.getResponseCode());
            recordResult.setResponseMessage(item.getResponseMessage());
            recordResult.setAttackType(item.getAiAttackType());
            recordResult.setIsSafeAttack(item.getAiTag());
            if (StringUtils.isNotEmpty(item.getPromptTokens1())) {
                recordResult.setClassifyBinaryPromptTokens(Integer.valueOf(item.getPromptTokens1()));
            }
            if (StringUtils.isNotEmpty(item.getTotalTokens1())) {
                recordResult.setClassifyBinaryTotalTokens(Integer.valueOf(item.getTotalTokens1()));
            }
            if (StringUtils.isNotEmpty(item.getCompletionTokens1())) {
                recordResult.setClassifyBinaryCompletionTokens(Integer.valueOf(item.getCompletionTokens1()));
            }
            if (StringUtils.isNotEmpty(item.getTotalTokens2())) {
                recordResult.setClassifyMultiTotalTokens(Integer.valueOf(item.getTotalTokens2()));
            }
            if (StringUtils.isNotEmpty(item.getPromptTokens2())) {
                recordResult.setClassifyMultiPromptTokens(Integer.valueOf(item.getPromptTokens2()));
            }
            if (StringUtils.isNotEmpty(item.getCompletionTokens2())) {
                recordResult.setClassifyMultiCompletionTokens(Integer.valueOf(item.getCompletionTokens2()));
            }
            if (StringUtils.isNotEmpty(item.getTotalTokens3())) {
                recordResult.setExtractInfoTotalTokens(Integer.valueOf(item.getTotalTokens3()));
            }
            if (StringUtils.isNotEmpty(item.getPromptTokens3())) {
                recordResult.setExtractInfoPromptTokens(Integer.valueOf(item.getPromptTokens3()));
            }
            if (StringUtils.isNotEmpty(item.getCompletionTokens3())) {
                recordResult.setExtractInfoCompletionTokens(Integer.valueOf(item.getCompletionTokens3()));
            }
            results.add(recordResult);
        });
      //  log.info("recordResult数据{}",results.get(0));
        return results;
    }

    @Override
    public AlarmRecordResult getNewData() {
        LambdaQueryWrapper<AlarmData2> wrapper = new LambdaQueryWrapper<>();
        wrapper.orderByDesc(AlarmData2::getID)  // 按ID倒序排列
                .last("LIMIT 1");

        AlarmData2 alarmData2 = alarmData2Dao.selectOne(wrapper);
        log.info("获取当前最新的告警数据{}",alarmData2);
        AlarmRecordResult recordResult = new AlarmRecordResult();
        recordResult.setId(alarmData2.getID());
        recordResult.setAlarmName(alarmData2.getAlarmName());
        recordResult.setSrcIp(alarmData2.getSrcIp());
        recordResult.setDstIp(alarmData2.getDstIp());
        recordResult.setAttackPort(Integer.valueOf(alarmData2.getSrcPort()));
        recordResult.setAlarmTime(alarmData2.getLastAlarmTime());
        recordResult.setAttackField(alarmData2.getKeyword());
        recordResult.setPayload(alarmData2.getPayload());
        recordResult.setAttackType(alarmData2.getAiAttackType());
        recordResult.setIsSafeAttack(alarmData2.getAiTag());
        recordResult.setClassifyBinaryPromptTokens(Integer.valueOf(alarmData2.getPromptTokens1()));
        recordResult.setClassifyBinaryTotalTokens(Integer.valueOf(alarmData2.getTotalTokens1()));
        recordResult.setClassifyBinaryCompletionTokens(Integer.valueOf(alarmData2.getCompletionTokens1()));
        recordResult.setClassifyMultiTotalTokens(Integer.valueOf(alarmData2.getTotalTokens2()));
        recordResult.setClassifyMultiPromptTokens(Integer.valueOf(alarmData2.getPromptTokens2()));
        recordResult.setClassifyMultiCompletionTokens(Integer.valueOf(alarmData2.getCompletionTokens2()));
        recordResult.setExtractInfoTotalTokens(Integer.valueOf(alarmData2.getTotalTokens3()));
        recordResult.setExtractInfoPromptTokens(Integer.valueOf(alarmData2.getPromptTokens3()));
        recordResult.setExtractInfoCompletionTokens(Integer.valueOf(alarmData2.getCompletionTokens3()));
        return recordResult;
    }

    @Override
    public AlarmRecordResult getOldData() {
        LambdaQueryWrapper<AlarmData2> wrapper = new LambdaQueryWrapper<>();
        wrapper.last("LIMIT 1");

        AlarmData2 alarmData2 = alarmData2Dao.selectOne(wrapper);
        log.info("获取当前最新的告警数据{}",alarmData2);
        AlarmRecordResult recordResult = new AlarmRecordResult();
        recordResult.setId(alarmData2.getID());
        recordResult.setAlarmName(alarmData2.getAlarmName());
        recordResult.setSrcIp(alarmData2.getSrcIp());
        recordResult.setDstIp(alarmData2.getDstIp());
        recordResult.setAttackPort(Integer.valueOf(alarmData2.getSrcPort()));
        recordResult.setAlarmTime(alarmData2.getLastAlarmTime());
        recordResult.setAttackField(alarmData2.getKeyword());
        recordResult.setPayload(alarmData2.getPayload());
        recordResult.setAttackType(alarmData2.getAiAttackType());
        recordResult.setIsSafeAttack(alarmData2.getAiTag());
        if(StringUtils.isNotEmpty(alarmData2.getPromptTokens1())){
            recordResult.setClassifyBinaryPromptTokens(Integer.valueOf(alarmData2.getPromptTokens1()));
        }
        if (StringUtils.isNotEmpty(alarmData2.getTotalTokens1())) {
            recordResult.setClassifyBinaryTotalTokens(Integer.valueOf(alarmData2.getTotalTokens1()));
        }
        if (StringUtils.isNotEmpty(alarmData2.getCompletionTokens1())) {
            recordResult.setClassifyBinaryCompletionTokens(Integer.valueOf(alarmData2.getCompletionTokens1()));
        }
        if (StringUtils.isNotEmpty(alarmData2.getTotalTokens2())) {
            recordResult.setClassifyMultiTotalTokens(Integer.valueOf(alarmData2.getTotalTokens2()));
        }
        if (StringUtils.isNotEmpty(alarmData2.getPromptTokens2())) {
            recordResult.setClassifyMultiPromptTokens(Integer.valueOf(alarmData2.getPromptTokens2()));
        }
        if (StringUtils.isNotEmpty(alarmData2.getCompletionTokens2())) {
            recordResult.setClassifyMultiCompletionTokens(Integer.valueOf(alarmData2.getCompletionTokens2()));
        }
        if (StringUtils.isNotEmpty(alarmData2.getTotalTokens3())) {
            recordResult.setExtractInfoTotalTokens(Integer.valueOf(alarmData2.getTotalTokens3()));
        }
        if (StringUtils.isNotEmpty(alarmData2.getPromptTokens3())) {
            recordResult.setExtractInfoPromptTokens(Integer.valueOf(alarmData2.getPromptTokens3()));
        }
        if (StringUtils.isNotEmpty(alarmData2.getCompletionTokens3())) {
            recordResult.setExtractInfoCompletionTokens(Integer.valueOf(alarmData2.getCompletionTokens3()));
        }
        return recordResult;
    }

    @Override
    public AlarmRecordResult importData() {
        LambdaQueryWrapper<AlarmRecordResult> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.between(AlarmRecordResult::getAlarmTime,"2025-09-23 00:00:00","2025-09-24 00:00:00");
        List<AlarmRecordResult> results = alarmRecordResultdao.selectList(queryWrapper);
        ArrayList<AlarmRecordResult2> result2s = new ArrayList<>();
        BeanUtils.copyProperties(results,result2s);
        log.info("数据条数{}",results.size());
        int total = result2s.size();
        if (total > 0) {
            int batch = 1200;
            int times = total/batch;

            for (int i=0;i<=times;i++) {
                if ( (i+1)*batch >= total) {
                    List<AlarmRecordResult2> subList = result2s.subList(i*batch,total);
                    alarmRecordResult2dao.insertBatch(subList);
                    log.info("save batch: {}", (i+1));
                } else {
                    List<AlarmRecordResult2> subList = result2s.subList(i*batch,(i+1)*batch);
                    alarmRecordResult2dao.insertBatch(subList);
                    log.info("save batch: {}", (i+1));
                }
            }
        }
        return null;
    }

    public List<AlarmData2> getRecordsGreaterThanId(Long startID,Long endID) {
        LambdaQueryWrapper<AlarmData2> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.between(AlarmData2::getID, startID, endID);
        // 按 ID 升序排序（可选，根据需求调整）
   //     queryWrapper.orderByAsc(AlarmData2::getID);
        return alarmData2Dao.selectList(queryWrapper);
    }
}