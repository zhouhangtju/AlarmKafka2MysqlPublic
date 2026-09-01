package com.mobile.safe.controller;


import cn.hutool.core.io.resource.ResourceUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.mobile.safe.dao.*;
import com.mobile.safe.db.*;
import com.mobile.safe.dto.PushResult;
import com.mobile.safe.service.*;
import com.mobile.safe.util.ThreadPoolUtil;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.bean.ColumnPositionMappingStrategy;
import com.opencsv.bean.CsvToBeanBuilder;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

@RestController
@RequestMapping("/test")
@Slf4j
@Api(value = "测试接口", tags = {"测试接口"})
public class DemoController {


    @Autowired
    private ResultService resultService;
    @Autowired
    private AlarmRecordResultDao alarmRecordResultDao;
    @Autowired
    private AlarmRecordResultCsvDao alarmRecordResultCsvDao;
    @Autowired
    private ReadAlarmDataService readAlarmDataService;

    @Autowired
    private AlarmRecordResultService alarmRecordResultService;
    @Autowired
    private AlarmData2Dao alarmData2Dao;
    @Autowired
    private TestDao testDao;
    @Autowired
    private AlarmData2Service alarmData2Service;

    @Autowired
    private KafkaTemplate kafkaTemplate;







    @GetMapping("/testGetALarmData")
    @ApiOperation("测试获取alarmData")
    public void testGetALarmData() {
        log.info("====执行获取告警数据任务===");
        LambdaQueryWrapper<AlarmData2> wrapper = new LambdaQueryWrapper<>();
        wrapper.orderByDesc(AlarmData2::getID)  // 按ID倒序排列
                .last("LIMIT 1");

        AlarmData2 alarmData2 = alarmData2Dao.selectOne(wrapper);

        LambdaQueryWrapper<AlarmRecordResult> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.orderByDesc(AlarmRecordResult::getId)  // 按ID倒序排列
                .last("LIMIT 1");
        AlarmRecordResult recordResult = alarmRecordResultDao.selectOne(queryWrapper);
        if(recordResult==null){
            return;
        }
        log.info("====当前ID区间{}-{}",recordResult.getId()+1,alarmData2.getID());
        List<AlarmRecordResult> results = readAlarmDataService.readAlarmData(recordResult.getId() + 1, alarmData2.getID());
        log.info("====当前ID区间条数{}",results.size());
        alarmRecordResultService.saveAlarmData(results);
    }


    @GetMapping("/getNewALarmData")
    @ApiOperation("获取最新的一条告警数据,存入数据库")
    public void getNewALarmData() {
        AlarmRecordResult newData = readAlarmDataService.getNewData();
        log.info("最新一条ALarmData{}",newData);
        alarmRecordResultDao.insert(newData);
    }

    @GetMapping("/getOldALarmData")
    @ApiOperation("获取最老的一条告警数据,存入数据库")
    public void getOldALarmData() {
        AlarmRecordResult newData = readAlarmDataService.getOldData();
        log.info("最新一条ALarmData{}",newData);
        alarmRecordResultDao.insert(newData);
    }
    private static final DateTimeFormatter DT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    @GetMapping("/updatePayload")
    public void updatePayload() {
        //LambdaQueryWrapper<AlarmRecordResult> wrapper = new LambdaQueryWrapper<>();
        Long minId = null;
        Long maxId = null;
        String startStr = "2025-11-25 00:00:00";
        String finalEndStr = "2025-11-28 20:00:00";

        // 2. 解析起始/最终结束时间（处理解析异常）
        LocalDateTime startTime;
        LocalDateTime finalEndTime;
        try {
            startTime = LocalDateTime.parse(startStr, DT_FORMATTER);
            finalEndTime = LocalDateTime.parse(finalEndStr, DT_FORMATTER);
        } catch (DateTimeParseException e) {
            System.err.println("时间格式解析失败！要求格式：yyyy-MM-dd HH:mm:ss，错误信息：" + e.getMessage());
            return;
        }

        // 3. 校验时间范围合法性
        if (startTime.isAfter(finalEndTime)) {
            System.err.println("起始时间不能晚于最终结束时间！");
            return;
        }

        // 4. 按30分钟切片循环查询
        LocalDateTime currentSliceStart = startTime;
        while (currentSliceStart.isBefore(finalEndTime)) {
            // 计算当前切片的结束时间（+30分钟），若超过最终结束时间则截断
            LocalDateTime currentSliceEnd = currentSliceStart.plusMinutes(30);
            if (currentSliceEnd.isAfter(finalEndTime)) {
                currentSliceEnd = finalEndTime;
            }

            // 5. 构建当前30分钟切片的查询条件
            LambdaQueryWrapper<AlarmRecordResult> wrapper = new LambdaQueryWrapper<>();
            // 注意：若getAlarmTime是LocalDateTime类型，直接传对象；若是String类型则格式化后传字符串
            wrapper.between(AlarmRecordResult::getAlarmTime,
                    currentSliceStart.format(DT_FORMATTER),  // 切片起始时间（字符串）
                    currentSliceEnd.format(DT_FORMATTER));   // 切片结束时间（字符串）

            List<AlarmRecordResult> results = alarmRecordResultDao.selectList(wrapper);
            log.info("当前处理时间{}",currentSliceEnd);
            log.info("当前处理条数{}",results.size());
            if (!results.isEmpty()) {
                // 提取最小ID
                minId = results.stream()
                        .map(AlarmRecordResult::getId) // 映射出所有id字段
                        .min(Long::compareTo) // 比较Long类型大小，取最小值
                        .orElse(null); // 无数据时返回null（此处已判空，实际不会触发）

                // 提取最大ID
                maxId = results.stream()
                        .map(AlarmRecordResult::getId)
                        .max(Long::compareTo)
                        .orElse(null);
            }
            List<AlarmRecordResult> resultList = readAlarmDataService.readAlarmData(minId, maxId);
            alarmRecordResultService.updateBatchById(resultList);
            currentSliceStart = currentSliceEnd;
        }
    }

    @GetMapping("/importData")
    @ApiOperation("获取昨天的数据")
    public void importData() {
        AlarmRecordResult newData = readAlarmDataService.importData();
        log.info("最新一条ALarmData{}",newData);
        alarmRecordResultDao.insert(newData);
    }

    @GetMapping("/testgroup")
    @ApiOperation("指定聚合一段时间的数据")
    public void testgroup(){

        List<AlarmRecordResult> results1 = alarmRecordResultDao.selectList(null);

        //List<AlarmRecordResult> results = alarmRecordResultService.getRecordResults(Long.parseLong(startId), Long.parseLong(endId));

        resultService.aggregateTenMin(results1);
    }



    @GetMapping("/pushResult")
    @ApiOperation("推送指定天的指标")
    public void test5(String dd) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        try {
            Date startTime = sdf.parse(dd);
            Date endTime = DateUtils.addDays(startTime, 1);
            resultService.pushResult(startTime,endTime);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }


    @GetMapping("/test9")
    @ApiOperation("推送指定天的指标")
    public void test9() {
        LambdaQueryWrapper<Test> queryWrapper = new LambdaQueryWrapper<>();
        List<Test> list = testDao.selectList(null);
        System.out.printf(String.valueOf(list.get(0)));
    }


    @GetMapping("/test2")
    @ApiOperation("读取csv文件入库")
    public void test2(){
        //String fileName = "/usr/local/safety/attack_data_042200.csv";
        String fileName = "D:\\work\\attack_data_042200.csv";
        RFC4180Parser parser = new RFC4180ParserBuilder()
                .build();
        // 创建映射策略并指定字段顺序
        try(CSVReader reader = new CSVReaderBuilder(new FileReader(fileName)).withCSVParser(parser).withSkipLines(1).build()){
            ColumnPositionMappingStrategy<AlarmRecordResultCsv> strategy = new ColumnPositionMappingStrategy<>();
            strategy.setType(AlarmRecordResultCsv.class);
            // 按照文件中字段的实际顺序设置映射

            strategy.setColumnMapping(new String[]{
                    "id",
                    "alarmTime",
                    "dstIp",
                    "srcIp",
                    "alarmName",
                    "payload",
                    "isSafeAttack",
                    "attackType",
                    "attackField",
                    "attackPort",
            });



            // 使用 CsvToBeanBuilder 将 CSV 数据转换为 Java 对象列表
            List<AlarmRecordResultCsv> list = new CsvToBeanBuilder<AlarmRecordResultCsv>(reader)
                    .withSkipLines(1)
                    .withMappingStrategy(strategy)
                    .build()
                    .parse();


            //分批插入
            int total = list.size();
            System.out.println("=="+total);
            int batch = 2000;
            int times = total/batch;
            ArrayList<AlarmRecordResultCsv> newAlarmDataList = new ArrayList<>();
            ArrayList<AlarmRecordResult> recordResults = new ArrayList<>();
            for (int i=0;i<=times;i++) {
                if ( (i+1)*batch >= total) {
                    List<AlarmRecordResultCsv> subList = list.subList(i*batch,total);
                    alarmRecordResultCsvDao.insertBatch(subList);
                    System.out.println("===========第"+String.valueOf(i)+"批");
                    newAlarmDataList.addAll(subList);
                } else {
                    List<AlarmRecordResultCsv> subList = list.subList(i*batch,(i+1)*batch);
                    alarmRecordResultCsvDao.insertBatch(subList);
                    System.out.println("===========第"+String.valueOf(i)+"批");
                    newAlarmDataList.addAll(subList);
                }
            }
//            for (int i = 0; i < 1; i++) {
//                AlarmRecordResultCsv alarmRecordResultCsv = newAlarmDataList.get(0);
//                AlarmRecordResult recordResult = new AlarmRecordResult();
//                BeanUtils.copyProperties(alarmRecordResultCsv,recordResult);
//                recordResults.add(recordResult);
//                log.info("recordResults",recordResults);
//                //kafkaProducer.send(recordResults);
//            }
//            bpNoRealTimePerformanceMainMapper.insertBatch(list);
        }  catch (Exception e) {
            System.out.printf(String.valueOf(e));
        }
    }


    @GetMapping("/test")
    @ApiOperation("推送指定天的指标")
    public void test8(String dd) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        try {
            Date startTime = sdf.parse(dd);
            Date endTime = DateUtils.addDays(startTime, 1);
            LambdaQueryWrapper<AlarmRecordResult> wrapper = new LambdaQueryWrapper<>();
            wrapper.ge(AlarmRecordResult::getAlarmTime,startTime);
            wrapper.lt(AlarmRecordResult::getAlarmTime,endTime);
            List<AlarmRecordResult> alarmRecordResults = alarmRecordResultDao.selectList(wrapper);
            System.out.printf(String.valueOf(alarmRecordResults.size()));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @GetMapping("/getResultMock")
    @ApiOperation("推送模拟指标")
    public PushResult test10() {
        PushResult resultMock = resultService.getResultMock();
        return resultMock;
    }

    @GetMapping("/test11")
    @ApiOperation("更新告警数据")
    public void test11() {
        log.info("====执行获取告警数据任务===");
        LambdaQueryWrapper<AlarmData2> wrapper = new LambdaQueryWrapper<>();
        wrapper.orderByDesc(AlarmData2::getID)  // 按ID倒序排列
                .last("LIMIT 1");

        AlarmData2 alarmData2 = alarmData2Dao.selectOne(wrapper);

        LambdaQueryWrapper<AlarmRecordResult> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.orderByDesc(AlarmRecordResult::getId)  // 按ID倒序排列
                .last("LIMIT 1");
        AlarmRecordResult recordResult = alarmRecordResultDao.selectOne(queryWrapper);
        if(recordResult==null){
            return;
        }
        log.info("====当前ID区间{}-{}",recordResult.getId()+1,alarmData2.getID());
        List<AlarmRecordResult> results = readAlarmDataService.readAlarmData(recordResult.getId() + 1, alarmData2.getID());
        Boolean b = alarmRecordResultService.saveAlarmData(results);
        if(b){
            log.info("获取告警数据任务成功");
        }else {
            log.error("获取告警数据任务失败");
        }
    }


    @GetMapping("/sendTestData")
    @ApiOperation("推送模拟指标")
    public void sendTestData() {
        String jsonData = ResourceUtil.readUtf8Str("test.json");
        kafkaTemplate.send("asap_superset", jsonData);
    }

}

