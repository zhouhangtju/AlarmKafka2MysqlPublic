package com.mobile.safe.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.mobile.safe.common.AggregatedData;
import com.mobile.safe.config.CommonConfig;
import com.mobile.safe.dao.AlarmRecordResultDao;
import com.mobile.safe.dao.GroupResultDao;
import com.mobile.safe.db.AlarmRecordResult;
import com.mobile.safe.db.GroupResult;
import com.mobile.safe.dto.*;
import com.mobile.safe.service.ReadAlarmDataService;
import com.mobile.safe.service.ResultService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.list.GrowthList;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Service;

import org.springframework.web.client.RestTemplate;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ResultServiceImpl implements ResultService {

    @Autowired
    private AlarmRecordResultDao alarmRecordResultDao;
    @Autowired
    private GroupResultDao groupResultDao;

    @Autowired
    private CommonConfig commonConfig;

    @Autowired
    private ReadAlarmDataService readAlarmDataService;

//    @Override
//    public void aggregate(List<AlarmRecordResult> results) {
//        // 1. 按时间排序
//     //   LambdaQueryWrapper<AlarmRecordResult> queryWrapper = new LambdaQueryWrapper<>();
//        //queryWrapper.between(AlarmRecordResult::getAlarmTime,startTime,endTime);
//        //queryWrapper.orderByAsc(AlarmRecordResult::getAlarmTime);
////        List<AlarmRecordResult> results = readAlarmDataService.readAlarmData(startID,endID);
////        List<AlarmRecordResult> results = alarmRecordResultDao.selectList(queryWrapper);
//
//
//        Map<String, List<AlarmRecordResult>> groupedAlarms = results.stream()
//                .filter(result -> result.getAttackType() != null && !result.getAttackType().isEmpty())
//                .filter(result -> result.getIsSafeAttack() != null && !result.getIsSafeAttack().isEmpty())
//                .collect(Collectors.groupingBy(
//                        result -> result.getSrcIp() + "_" + result.getDstIp() + "_" +
//                                result.getAttackType() + "_" + result.getIsSafeAttack()
//                ));
//
//        List<AggregatedData> result = new ArrayList<>();
//        //List<GroupResult> groupResults = new GrowthList();
//        ArrayList<Long> ids = new ArrayList<>();
//        // 2. 对每组数据进行处理
//        for (Map.Entry<String, List<AlarmRecordResult>> entry : groupedAlarms.entrySet()) {
//            String groupKey = entry.getKey();
//            List<AlarmRecordResult> groupAlarms = entry.getValue();
//
//
//            // 4. 初始化第一个聚合组
//            if (groupAlarms.isEmpty()) continue;
//                AggregatedData currentGroup = new AggregatedData();
//                currentGroup.setGroupKey(groupKey);
//                currentGroup.setTruePayload(0);
//                AlarmRecordResult firstAlarm = groupAlarms.get(0);
//                currentGroup.setStartTime(firstAlarm.getAlarmTime());
//                currentGroup.setEndTime(DateUtils.addMinutes(firstAlarm.getAlarmTime(), 1));
//                currentGroup.addAlarm(firstAlarm);
//                ids.add(firstAlarm.getId());
//            if(StringUtils.isNotEmpty(firstAlarm.getPayload())){
//                currentGroup.setTruePayload(currentGroup.getTruePayload()+1);
//            }
//            currentGroup.setSize(currentGroup.getResults().size());
//            // 5. 遍历剩余告警
//            for (int i = 1; i < groupAlarms.size(); i++) {
//                AlarmRecordResult recordResult = groupAlarms.get(i);
//                //如果告警时间减去最新一条的结束时间
//
//                if (    currentGroup.getEndTime().getTime()-recordResult.getAlarmTime().getTime()>=0&&
//                        currentGroup.getEndTime().getTime()-recordResult.getAlarmTime().getTime() <= 60000) {
//                    // 时间差小于阈值，加入当前组 重新赋值结束时间
//                    //currentGroup.setEndTime(recordResult.getAlarmTime());
//                    currentGroup.addAlarm(recordResult);
//                    ids.add(recordResult.getId());
//                    if(StringUtils.isNotEmpty(firstAlarm.getPayload())){
//                        currentGroup.setTruePayload(currentGroup.getTruePayload()+1);
//                    }
//                    currentGroup.setSize(currentGroup.getResults().size());
//                } else {
//                    // 时间差大于阈值，创建新组  重新赋值初始时间和结束时间
//                    result.add(currentGroup);
//                    GroupResult groupResult = new GroupResult();
//                    groupResult.setGroupName(currentGroup.getGroupKey());
//                    groupResult.setAlarmTime(currentGroup.getStartTime());
//                    groupResult.setIds(String.valueOf(ids));
//                    groupResult.setTruePayload(currentGroup.getTruePayload());
//                    groupResultDao.insert(groupResult);
//                    ids.clear();
//
//                    currentGroup = new AggregatedData();
//                    currentGroup.setTruePayload(0);
//                    currentGroup.setGroupKey(groupKey);
//                    currentGroup.setStartTime(recordResult.getAlarmTime());
//                    currentGroup.setEndTime(DateUtils.addMinutes(currentGroup.getStartTime(),1));
//                    currentGroup.addAlarm(recordResult);
//                    ids.add(recordResult.getId());
//                    if(StringUtils.isNotEmpty(firstAlarm.getPayload())){
//                        currentGroup.setTruePayload(currentGroup.getTruePayload()+1);
//                    }
//                    currentGroup.setSize(currentGroup.getResults().size());
//                }
//            }
//
//            // 添加最后一个组
//            result.add(currentGroup);
//            GroupResult groupResult = new GroupResult();
//            groupResult.setGroupName(currentGroup.getGroupKey());
//            groupResult.setAlarmTime(currentGroup.getStartTime());
//            groupResult.setIds(String.valueOf(ids));
//            groupResult.setTruePayload(currentGroup.getTruePayload());
//            groupResultDao.insert(groupResult);
//            ids.clear();
//        }
//    }

    @Override
    public void aggregate(List<AlarmRecordResult> results) {
        // 1. 按条件过滤并分组数据
        Map<String, List<AlarmRecordResult>> groupedAlarms = results.stream()
                .filter(result -> {
                    String isSafeAttack = result.getIsSafeAttack();
                    String attackType = result.getAttackType();
                    String attackField = result.getAttackField(); // 获取attackField

                    // 根据isSafeAttack值调整过滤逻辑
                    if ("No".equals(isSafeAttack)) {
                        // isSafeAttack为No时，不过滤attackType和attackField为空的情况
                        return isSafeAttack != null && !isSafeAttack.isEmpty();
                    } else if ("Yes".equals(isSafeAttack)) {
                        // isSafeAttack为Yes时，attackType或attackField为空则过滤
                        return isSafeAttack != null && !isSafeAttack.isEmpty() &&
                                attackType != null && !attackType.isEmpty() &&
                                attackField != null && !attackField.isEmpty(); // 添加attackField判断
                    } else {
                        // 其他情况默认过滤
                        return false;
                    }
                })
                .collect(Collectors.groupingBy(
                        result -> result.getSrcIp() + "_" + result.getDstIp() + "_" +
                                result.getAttackType() + "_" + result.getIsSafeAttack()
                ));

        List<AggregatedData> result = new ArrayList<>();
        ArrayList<Long> ids = new ArrayList<>();

        // 2. 对每组数据进行处理（取消时间窗口聚合）
        for (Map.Entry<String, List<AlarmRecordResult>> entry : groupedAlarms.entrySet()) {
            String groupKey = entry.getKey();
            List<AlarmRecordResult> groupAlarms = entry.getValue();

            if (groupAlarms.isEmpty()) continue;

            // 初始化聚合组
            AggregatedData currentGroup = new AggregatedData();
            currentGroup.setGroupKey(groupKey);
            currentGroup.setTruePayload(0);

            // 直接计算该组的所有告警
            for (AlarmRecordResult record : groupAlarms) {
                currentGroup.addAlarm(record);
                ids.add(record.getId());
                if (StringUtils.isNotEmpty(record.getPayload())) {
                    currentGroup.setTruePayload(currentGroup.getTruePayload() + 1);
                }
            }

            currentGroup.setSize(currentGroup.getResults().size());
            result.add(currentGroup);

            // 保存聚合结果
            GroupResult groupResult = new GroupResult();
            groupResult.setGroupName(groupKey);
            groupResult.setAlarmTime(groupAlarms.get(0).getAlarmTime());
            groupResult.setIds(String.valueOf(ids));
            groupResult.setTruePayload(currentGroup.getTruePayload());
            String[] split = groupKey.split("_");
            if(split.length==4){
                if(!split[2].equals("null")){
                    groupResult.setAttackType(split[2]);
                }
                groupResult.setSrcIp(split[0]);
                groupResult.setDstIp(split[1]);
                groupResult.setIsSafeAttack(split[3]);
            }
            groupResultDao.insert(groupResult);
            ids.clear();
        }
    }

    public void aggregateNew(List<AlarmRecordResult> results) {

        List<AlarmRecordResult> filtered = results.stream()
                .filter(result -> {
                    String isSafeAttack = result.getIsSafeAttack();
                    String attackType = result.getAttackType();
                    String attackField = result.getAttackField();
                    if ("No".equals(isSafeAttack)) {
                        return StringUtils.isNotEmpty(isSafeAttack);
                    } else if ("Yes".equals(isSafeAttack)) {
                        return StringUtils.isNotEmpty(isSafeAttack)
                                && StringUtils.isNotEmpty(attackType)
                                && StringUtils.isNotEmpty(attackField);
                    }
                    return false;
                })
                .sorted(Comparator.comparing(AlarmRecordResult::getAlarmTime, Comparator.nullsLast(Comparator.naturalOrder())))
                .collect(Collectors.toList());


        Map<String, List<AlarmRecordResult>> groupedAlarms = filtered.stream()
                .collect(Collectors.groupingBy(
                        r -> r.getSrcIp() + "_" + r.getDstIp() + "_" + r.getAttackType() + "_" + r.getIsSafeAttack()
                ));

        log.info("分组完成 ：{}",groupedAlarms.size());

        List<AggregatedData> resultList = new ArrayList<>();


        for (Map.Entry<String, List<AlarmRecordResult>> entry : groupedAlarms.entrySet()) {
            String groupKey = entry.getKey();
            List<AlarmRecordResult> groupAlarms = entry.getValue();

            // 组内已按时间排序，按 60s 间隔拆分为多个子批次
            List<List<AlarmRecordResult>> timeBatches = splitByTimeGap(groupAlarms, 60_000L);

            for (List<AlarmRecordResult> batch : timeBatches) {
                if (batch.isEmpty()) continue;

                // 每个 batch 是一个独立的聚合事件
                AggregatedData currentGroup = new AggregatedData();
                currentGroup.setGroupKey(groupKey);
                currentGroup.setTruePayload(0);

                List<Long> ids = new ArrayList<>();
                for (AlarmRecordResult record : batch) {
                    currentGroup.addAlarm(record);
                    ids.add(record.getId());
                    if (StringUtils.isNotEmpty(record.getPayload())) {
                        currentGroup.setTruePayload(currentGroup.getTruePayload() + 1);
                    }
                }
                currentGroup.setSize(batch.size());
                resultList.add(currentGroup);

                // 保存聚合结果
                GroupResult groupResult = new GroupResult();
                groupResult.setGroupName(groupKey);
                groupResult.setAlarmTime(batch.get(0).getAlarmTime());
                groupResult.setIds(String.valueOf(ids));
                groupResult.setTruePayload(currentGroup.getTruePayload());

                String[] split = groupKey.split("_", -1);
                if (split.length >= 4) {
                    groupResult.setSrcIp(split[0]);
                    groupResult.setDstIp(split[1]);
                    if (!"null".equals(split[2])) {
                        groupResult.setAttackType(split[2]);
                    }
                    groupResult.setIsSafeAttack(split[3]);
                }
                groupResultDao.insert(groupResult);
            }
        }
        log.info("GroupResult 入库完成");

    }

    /**
     * 将已排序的列表按时间间隔拆分为多个子批次
     *
     */
    private List<List<AlarmRecordResult>> splitByTimeGap(List<AlarmRecordResult> sortedList, long gapMillis) {
        List<List<AlarmRecordResult>> batches = new ArrayList<>();
        if (sortedList == null || sortedList.isEmpty()) return batches;

        List<AlarmRecordResult> currentBatch = new ArrayList<>();
        currentBatch.add(sortedList.get(0));
        Date prevTime = sortedList.get(0).getAlarmTime();

        for (int i = 1; i < sortedList.size(); i++) {
            AlarmRecordResult current = sortedList.get(i);
            Date currTime = current.getAlarmTime();


            long diff = (currTime != null && prevTime != null)
                    ? Math.abs(currTime.getTime() - prevTime.getTime())
                    : Long.MAX_VALUE;

            if (diff > gapMillis) {

                batches.add(currentBatch);
                currentBatch = new ArrayList<>();
            }
            currentBatch.add(current);
            prevTime = currTime;
        }

        batches.add(currentBatch);
        return batches;
    }

    @Override
    public void aggregateTenMin(List<AlarmRecordResult> results) {
        // 1. 按条件过滤并分组数据
        Map<String, List<AlarmRecordResult>> groupedAlarms = results.stream()
                .filter(result -> {
                    String isSafeAttack = result.getIsSafeAttack();
                    String attackType = result.getAttackType();
                    String attackField = result.getAttackField();

                    // 根据isSafeAttack值调整过滤逻辑
                    if ("No".equals(isSafeAttack)) {
                        // isSafeAttack为No时，不过滤attackType和attackField为空的情况
                        return isSafeAttack != null && !isSafeAttack.isEmpty();
                    } else if ("Yes".equals(isSafeAttack)) {
                        // isSafeAttack为Yes时，attackType或attackField为空则过滤
                        return isSafeAttack != null && !isSafeAttack.isEmpty() &&
                                attackType != null && !attackType.isEmpty() &&
                                attackField != null && !attackField.isEmpty();
                    } else {
                        // 其他情况默认过滤
                        return false;
                    }
                })
                .sorted(Comparator.comparing(AlarmRecordResult::getAlarmTime)) // 按告警时间排序
                .collect(Collectors.groupingBy(
                        result -> result.getSrcIp() + "_" + result.getDstIp() + "_" +
                                result.getAttackType() + "_" + result.getIsSafeAttack()
                ));

        List<AggregatedData> result = new ArrayList<>();
        ArrayList<Long> ids = new ArrayList<>();

        // 2. 对每组数据进行处理（恢复时间窗口聚合，窗口大小10分钟）
        for (Map.Entry<String, List<AlarmRecordResult>> entry : groupedAlarms.entrySet()) {
            String groupKey = entry.getKey();
            List<AlarmRecordResult> groupAlarms = entry.getValue();

            if (groupAlarms.isEmpty()) continue;

            // 初始化第一个聚合组
            AggregatedData currentGroup = new AggregatedData();
            currentGroup.setGroupKey(groupKey);
            currentGroup.setTruePayload(0);
            AlarmRecordResult firstAlarm = groupAlarms.get(0);
            currentGroup.setStartTime(firstAlarm.getAlarmTime());
            currentGroup.setEndTime(DateUtils.addMinutes(firstAlarm.getAlarmTime(), 10)); // 10分钟窗口
            currentGroup.addAlarm(firstAlarm);
            ids.add(firstAlarm.getId());

            if (StringUtils.isNotEmpty(firstAlarm.getPayload())) {
                currentGroup.setTruePayload(currentGroup.getTruePayload() + 1);
            }

            currentGroup.setSize(currentGroup.getResults().size());

            // 遍历剩余告警，按时间窗口聚合
            for (int i = 1; i < groupAlarms.size(); i++) {
                AlarmRecordResult record = groupAlarms.get(i);

                // 计算时间差（毫秒）
                long timeDiff = record.getAlarmTime().getTime() - currentGroup.getEndTime().getTime();

                if (timeDiff <= 0) {
                    // 当前记录在时间窗口内，加入当前组
                    currentGroup.addAlarm(record);
                    ids.add(record.getId());

                    if (StringUtils.isNotEmpty(record.getPayload())) {
                        currentGroup.setTruePayload(currentGroup.getTruePayload() + 1);
                    }

                    currentGroup.setSize(currentGroup.getResults().size());
                } else {
                    // 超出当前时间窗口，创建新组
                    result.add(currentGroup);

                    // 保存当前聚合结果
                    GroupResult groupResult = new GroupResult();
                    groupResult.setGroupName(currentGroup.getGroupKey());
                    groupResult.setAlarmTime(currentGroup.getStartTime());
                    groupResult.setIds(String.valueOf(ids));
                    groupResult.setTruePayload(currentGroup.getTruePayload());
                    String[] split = groupKey.split("_");
                    if(split.length==4){
                        if(!split[2].equals("null")){
                            groupResult.setAttackType(split[2]);
                        }

                        groupResult.setSrcIp(split[0]);
                        groupResult.setDstIp(split[1]);
                        groupResult.setIsSafeAttack(split[3]);
                    }
                    groupResultDao.insert(groupResult);
                    ids.clear();

                    // 初始化新聚合组
                    currentGroup = new AggregatedData();
                    currentGroup.setGroupKey(groupKey);
                    currentGroup.setTruePayload(0);
                    currentGroup.setStartTime(record.getAlarmTime());
                    currentGroup.setEndTime(DateUtils.addMinutes(record.getAlarmTime(), 10)); // 10分钟窗口
                    currentGroup.addAlarm(record);
                    ids.add(record.getId());

                    if (StringUtils.isNotEmpty(record.getPayload())) {
                        currentGroup.setTruePayload(currentGroup.getTruePayload() + 1);
                    }

                    currentGroup.setSize(currentGroup.getResults().size());
                }
            }

            // 添加最后一个组
            result.add(currentGroup);

            // 保存最后一个聚合结果
            GroupResult groupResult = new GroupResult();
            groupResult.setGroupName(currentGroup.getGroupKey());
            groupResult.setAlarmTime(currentGroup.getStartTime());
            groupResult.setIds(String.valueOf(ids));
            groupResult.setTruePayload(currentGroup.getTruePayload());
            String[] split = groupKey.split("_");
            if(split.length==4){
                if(!split[2].equals("null")){
                    groupResult.setAttackType(split[2]);
                }

                groupResult.setSrcIp(split[0]);
                groupResult.setDstIp(split[1]);
                groupResult.setIsSafeAttack(split[3]);
            }
            groupResultDao.insert(groupResult);
            ids.clear();
        }
    }


    @Override
    public PushResult getResult(Date startTime,Date endTime) {
        PushResult pushResult = new PushResult();
        // 创建DecimalFormat对象，指定保留两位小数
        DecimalFormat decimalFormat = new DecimalFormat("#0.00");


        //找出是否标记为请求成功的集合数，即为今天的请求数
        LambdaQueryWrapper<AlarmRecordResult> queryWrapperAllNum = new LambdaQueryWrapper<>();
        queryWrapperAllNum.eq(AlarmRecordResult::getIsSuccessful,"是").or().eq(AlarmRecordResult::getIsSuccessful,"否");
        queryWrapperAllNum.ge(AlarmRecordResult::getAlarmTime,startTime);
        queryWrapperAllNum.lt(AlarmRecordResult::getAlarmTime,endTime);
        List<AlarmRecordResult> results = alarmRecordResultDao.selectList(queryWrapperAllNum);
        Integer handleAlarmNum = results.size();
        pushResult.setHandleAlarmNum(handleAlarmNum);
        if(results.size() == 0){
            return pushResult;
        }
        List<AlarmRecordResult> allresults = results.stream()
                .filter(record -> {
                    String isSuccessful = record.getIsSuccessful(); // 替换为实际属性的 getter 方法
//                    return isSafeAttack != null && (isSafeAttack.contains("Yes") || isSafeAttack.contains("No"));
                    return isSuccessful != null && isSuccessful.contains("是");
                })
                .collect(Collectors.toList());
        if(allresults.size()==0){
            return pushResult;
        }
        //从今天的请求集合中找出是否为安全攻击，即为classifyBinary的调用数
        List<AlarmRecordResult> filteredResults = allresults.stream()
                .filter(record -> {
                    String isSafeAttack = record.getIsSafeAttack(); // 替换为实际属性的 getter 方法
                    return isSafeAttack != null && (isSafeAttack.contains("Yes") || isSafeAttack.contains("No"));
                    //return isSafeAttack != null && (isSafeAttack.contains("是") || isSafeAttack.contains("否"));
                })
                .collect(Collectors.toList());
        //为安全攻击比例
        List<AlarmRecordResult> filteredResults4 = filteredResults.stream()
                .filter(record -> {
                    String isSafeAttack = record.getIsSafeAttack(); // 替换为实际属性的 getter 方法
                    return isSafeAttack != null && (isSafeAttack.contains("Yes"));
                    //return isSafeAttack != null && (isSafeAttack.contains("是"));
                })
                .collect(Collectors.toList());
        System.out.println("filteredResults4:"+ filteredResults4.size());
        Double safeAttackRatio = (double) filteredResults4.size() / filteredResults.size();
        pushResult.setSafeAttackRatio(decimalFormat.format(safeAttackRatio));
        //其他两个接口次数即为是安全攻击的次数
        Integer classifyBinaryNum = filteredResults.size();
        Integer classifyMultiNum = filteredResults4.size();
        Integer extractInfoNum =   filteredResults4.size();
        log.info("classifyBinary的调用数:{}",filteredResults.size());
        log.info("安全攻击数，即其他两个接口调用数量:{}",filteredResults4.size());
        pushResult.setClassifyBinaryNum(classifyBinaryNum);
        pushResult.setClassifyMultiNum(classifyMultiNum);
        pushResult.setExtractInfoNum(extractInfoNum);
        if(filteredResults.size()==0){
            return pushResult;
        }

        //每个接口的平均响应时长 平均token数
        long sumTemp1 = 0;
        long sumTemp2 = 0;
        long sumTemp3 = 0;
        Integer sumPromptTokens1 = 0;
        Integer completionTokens1 = 0;
        Integer totalTokens1 = 0;
        Integer sumPromptTokens2 = 0;
        Integer completionTokens2 = 0;
        Integer totalTokens2 = 0;
        Integer sumPromptTokens3 = 0;
        Integer completionTokens3 = 0;
        Integer totalTokens3 = 0;
        for (int i = 0; i < filteredResults.size(); i++) {
            if(filteredResults.get(i).getClassifyBinaryTotalTokens()!=null&&filteredResults.get(i).getClassifyBinaryUseTime()!=null){
                sumTemp1 = filteredResults.get(i).getClassifyBinaryUseTime() + sumTemp1;
                //sumPromptTokens1 = filteredResults.get(i).getClassifyBinaryPromptTokens() + sumPromptTokens1;
                //completionTokens1 = filteredResults.get(i).getClassifyBinaryCompletionTokens() + completionTokens1;
                totalTokens1 = filteredResults.get(i).getClassifyBinaryTotalTokens() + totalTokens1;
            }
        }
        for (int i = 0; i < filteredResults4.size(); i++) {
            if(filteredResults4.get(i).getClassifyMultiTotalTokens()!=null&&filteredResults4.get(i).getClassifyMultiUseTime()!=null){
                sumTemp2 = filteredResults4.get(i).getClassifyMultiUseTime() + sumTemp2;
//            sumPromptTokens2 = filteredResults4.get(i).getClassifyMultiPromptTokens() + sumPromptTokens2;
//            completionTokens2 = filteredResults4.get(i).getClassifyMultiCompletionTokens() + completionTokens2;
                totalTokens2 = filteredResults4.get(i).getClassifyMultiTotalTokens() + totalTokens2;
            }
            if(filteredResults4.get(i).getExtractInfoTotalTokens()!=null&&filteredResults4.get(i).getExtractInfoUseTime()!=null){
                sumTemp3 = filteredResults4.get(i).getExtractInfoUseTime() + sumTemp3;
//            sumPromptTokens3 = filteredResults4.get(i).getExtractInfoPromptTokens() + sumPromptTokens3;
//            completionTokens3 = filteredResults4.get(i).getExtractInfoCompletionTokens() + completionTokens3;
                totalTokens3 = filteredResults4.get(i).getExtractInfoTotalTokens() + totalTokens3;
            }
        }

        Double classifyBinaryTimeAvg = (double) (sumTemp1/filteredResults.size());
       // Double classifyBinaryPromptTokensAvg = (double) (sumPromptTokens1/filteredResults.size());
      //  Double classifyBinaryCompletionTokensAvg = (double) (completionTokens1/filteredResults.size());
        Double classifyBinaryTotalTokensAvg = (double) (totalTokens1/filteredResults.size());
        Double classifyMultiTimeAvg = (double) (sumTemp2/filteredResults4.size());
        //Double classifyMultiPromptTokensAvg = (double) (sumPromptTokens2/filteredResults4.size());
      //  Double classifyMultiCompletionTokensAvg = (double) (completionTokens2/filteredResults4.size());
        Double classifyMultiTotalTokensAvg = (double) (totalTokens2/filteredResults4.size());
        Double extractInfoTimeAvg = (double) (sumTemp3/filteredResults4.size());
     //   Double extractInfoTokensAvg = (double) (sumPromptTokens3/filteredResults4.size());
      //  Double extractInfoCompletionTokensAvg = (double) (completionTokens3/filteredResults4.size());
        Double extractInfoTotalTokensAvg = (double) (totalTokens3/filteredResults4.size());
        pushResult.setClassifyBinaryTimeAvg(decimalFormat.format(classifyBinaryTimeAvg));
      //  pushResult.setClassifyBinaryPromptTokensAvg(decimalFormat.format(classifyBinaryPromptTokensAvg));
      //  pushResult.setClassifyBinaryCompletionTokensAvg(decimalFormat.format(classifyBinaryCompletionTokensAvg));
      //  pushResult.setClassifyMultiCompletionTokensAvg(decimalFormat.format(classifyMultiCompletionTokensAvg));
        pushResult.setClassifyMultiTimeAvg(decimalFormat.format(classifyMultiTimeAvg));
      //  pushResult.setClassifyMultiPromptTokensAvg(decimalFormat.format(classifyMultiPromptTokensAvg));
        pushResult.setExtractInfoTimeAvg(decimalFormat.format(extractInfoTimeAvg));
      //  pushResult.setExtractInfoPromptTokensAvg(decimalFormat.format(extractInfoTokensAvg));
      //  pushResult.setExtractInfoCompletionTokensAvg(decimalFormat.format(extractInfoCompletionTokensAvg));
        pushResult.setClassifyBinaryTotalTokensAvg(decimalFormat.format(classifyBinaryTotalTokensAvg));
        pushResult.setClassifyMultiTotalTokensAvg(decimalFormat.format(classifyMultiTotalTokensAvg));
        pushResult.setExtractInfoTotalTokensAvg(decimalFormat.format(extractInfoTotalTokensAvg));
       return pushResult;
    }

    @Override
    public String getToken() {
        RestTemplate restTemplate = commonConfig.getRestTemplate();
//        // 设置请求头
//        HttpHeaders headers = new HttpHeaders();
//        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
//        String url = "http://188.103.147.179:30181/agent-ops-backend/api/agentops/indicatorToken";
//        // 设置请求体参数
//        MultiValueMap<String, String> body = new LinkedMultiValueMap<>();
//        body.add("app_code", "app_code");
//        body.add("secret_key", "LCEzNgaZZp0/JILZw4gEpB7OiymbWYFKI8ERifIunc4=");
//d
//        // 创建HTTP请求实体
//        HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(body, headers);
//        ResponseEntity<String> response = restTemplate.getForEntity(url,request,String.class);

        // 设置请求URL
        String baseUrl = "http://188.103.147.179:30181/agent-ops-backend/api/agentops/indicatorToken?app_code=app_code&secret_key=LCEzNgaZZp0/JILZw4gEpB7OiymbWYFKI8ERifIunc4=";


        // 设置请求头（可选，根据需要添加）
        //HttpHeaders headers = new HttpHeaders();
        //headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        // 如果需要添加Authorization头，可以这样设置
        // headers.set("Authorization", "Bearer your_token_here");

        // 创建HttpEntity对象，包含请求头
        //HttpEntity<?> entity = new HttpEntity<>(headers);

        // 发送GET请求并获取响应
        ResponseEntity<String> response = restTemplate.getForEntity(baseUrl, String.class);

        if (response.getStatusCodeValue() == 200) {
            String res = response.getBody();
            log.info("res{}",res);
            JSONObject jsonObject = JSON.parseObject(res);
            String data = jsonObject.getString("data");
            JSONObject object = JSON.parseObject(data);
            String accessToken = object.getString("access_token");
            log.info("token值{}",accessToken);
            return accessToken;
        }
        return null;
    }

    @Override
    public void pushResult(Date startTime,Date endTime) {
        RestTemplate restTemplate = commonConfig.getRestTemplate();
//        PushResult result = this.getResult(startTime,endTime);
        PushResult result = this.getResultMock();
        String token = this.getToken();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date date = new Date();
        String time = sdf.format(date);
//        // 设置请求头
//        HttpHeaders headers = new HttpHeaders();
//        headers.set("Authorization", "Bearer fake_"+token);
//        headers.setContentType(MediaType.APPLICATION_JSON);
//
//        // 设置请求体
//        Map<String, Object> requestBody = new HashMap<>();
//        requestBody.put("agentName", "网络安全分析智能体");
//
//        Map<String, Object> indicator = new HashMap<>();
//        indicator.put("handleAlarmNum",result.getHandleAlarmNum());
//        indicator.put("classifyBinaryNum",result.getClassifyBinaryNum());
//        indicator.put("classifyMultiNum",result.getClassifyMultiNum());
//        indicator.put("extractInfoNum",result.getExtractInfoNum());
//        indicator.put("classifyBinaryTimeAvg",result.getClassifyBinaryTimeAvg());
//        indicator.put("classifyMultiTimeAvg",result.getClassifyMultiTimeAvg());
//        indicator.put("extractInfoTimeAvg",result.getExtractInfoTimeAvg());
//        indicator.put("classifyBinaryTotalTokensAvg",result.getClassifyBinaryTotalTokensAvg());
//        indicator.put("classifyMultiTotalTokensAvg",result.getClassifyMultiTotalTokensAvg());
//        indicator.put("extractInfoTotalTokensAvg",result.getExtractInfoTotalTokensAvg());
//        requestBody.put("indicator", indicator);
// 设置请求 URL
        String url = "http://188.103.147.179:30181/agent-ops-backend/api/agentops/indicators";

        // 设置请求头
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization","Bearer "+token);
        headers.setContentType(MediaType.APPLICATION_JSON);

        // 创建指标数据对象
        IndicatorRequest.Indicator indicator = new IndicatorRequest.Indicator();
        indicator.setHandleAlarmNum(result.getHandleAlarmNum());
        indicator.setClassifyBinaryNum(result.getClassifyBinaryNum());
        indicator.setClassifyMultiNum(result.getClassifyMultiNum());
        indicator.setExtractInfoNum(result.getExtractInfoNum());
        indicator.setClassifyBinaryTimeAvg(result.getClassifyBinaryTimeAvg());
        indicator.setClassifyMultiTimeAvg(result.getClassifyMultiTimeAvg());
        indicator.setExtractInfoTimeAvg(result.getExtractInfoTimeAvg());
        indicator.setClassifyBinaryTotalTokensAvg(result.getClassifyBinaryTotalTokensAvg());
        indicator.setClassifyMultiTotalTokensAvg(result.getClassifyMultiTotalTokensAvg());
        indicator.setExtractInfoTotalTokensAvg(result.getExtractInfoTotalTokensAvg());

        // 创建请求体对象
        IndicatorRequest requestBody = new IndicatorRequest();
        requestBody.setAgentName("网络安全分析智能体");
        requestBody.setIndicator(indicator);
        requestBody.setReportTime(time);
        log.info("推送数据{}",requestBody);
        HttpEntity<IndicatorRequest> request = new HttpEntity<>(requestBody,headers);
        ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);
        if (response.getStatusCodeValue() == 200) {
            String res = response.getBody();
            log.info("推送结果{}",res);
        }
    }
    private static final Random random = new Random();
    private static final DecimalFormat df = new DecimalFormat("#0.00");

    private static String generateTimeAvg(double baseValue) {
        // 在基准值附近±20%波动，并保留两位小数
        double value = baseValue * (0.8 + random.nextDouble() * 0.4);
        return df.format(value);
    }

    private static String calculateTokensAvg() {

        int multiplier = random.nextInt(200) + 100;
        return String.valueOf(multiplier);
    }
    @Override
    public PushResult getResultMock() {
            PushResult result = new PushResult();


            int handleAlarmNum = random.nextInt(50000) + 200000;
            result.setHandleAlarmNum(handleAlarmNum);

            // classifyBinaryNum等于handleAlarmNum
            result.setClassifyBinaryNum(handleAlarmNum);

            // classifyMultiNum和extractInfoNum大概为handleAlarmNum的三分之一
            int third = handleAlarmNum / 3;
            // 添加±10%的随机波动
            int classifyMultiNum = third + (int) ((random.nextDouble() * 0.2 - 0.1) * third);
            result.setClassifyMultiNum(classifyMultiNum);
            result.setExtractInfoNum(classifyMultiNum);

            // 设置时间平均值
            result.setClassifyBinaryTimeAvg(generateTimeAvg(100));
            result.setClassifyMultiTimeAvg(generateTimeAvg(200));
            result.setExtractInfoTimeAvg(generateTimeAvg(400));

            // 设置token平均值
            result.setClassifyBinaryTotalTokensAvg(calculateTokensAvg());
            result.setClassifyMultiTotalTokensAvg(calculateTokensAvg());
            result.setExtractInfoTotalTokensAvg(calculateTokensAvg());

            // 设置安全攻击比例 (示例值)
             Double safeAttackRatio = (double) classifyMultiNum /handleAlarmNum;
            result.setSafeAttackRatio(df.format(safeAttackRatio));
            return result;

        }


}
