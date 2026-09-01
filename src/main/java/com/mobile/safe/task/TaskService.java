package com.mobile.safe.task;


import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.mobile.safe.dao.AlarmData2Dao;
import com.mobile.safe.dao.AlarmRecordResultDao;
import com.mobile.safe.db.AlarmData2;
import com.mobile.safe.db.AlarmRecordResult;
import com.mobile.safe.service.AlarmRecordResultService;
import com.mobile.safe.service.ReadAlarmDataService;
import com.mobile.safe.service.ResultService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@Slf4j
public class TaskService{
    private final String taskExecutor = "taskExecutor";

    @Autowired
    private ResultService resultService;

    @Autowired
    private ReadAlarmDataService readAlarmDataService;

    @Autowired
    private AlarmRecordResultService alarmRecordResultService;

    @Autowired
    private AlarmData2Dao alarmData2Dao;

    @Autowired
    private AlarmRecordResultDao alarmRecordResultDao;


    @Async("myTaskExecutor")
//    @Scheduled(cron = "0 */10 * * * ?")  //每10分钟执行一次
    public void insertAndUpdateAlarmDataTask() {
        log.info("====执行获取告警数据任务===");
        LambdaQueryWrapper<AlarmData2> wrapper = new LambdaQueryWrapper<>();
        wrapper.orderByDesc(AlarmData2::getID)  // 按ID倒序排列
                .last("LIMIT 1");
        LambdaQueryWrapper<AlarmRecordResult> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.orderByDesc(AlarmRecordResult::getId)  // 按ID倒序排列
                .last("LIMIT 1");
        AlarmData2 alarmData2 = alarmData2Dao.selectOne(wrapper);
        AlarmRecordResult recordResult = alarmRecordResultDao.selectOne(queryWrapper);
        if(recordResult==null){
            return;
        }
        try {
        log.info("====当前ID区间{}-{}",recordResult.getId()+1,alarmData2.getID());
        List<AlarmRecordResult> results = readAlarmDataService.readAlarmData(recordResult.getId() + 1, alarmData2.getID());
        Boolean b = alarmRecordResultService.saveAlarmData(results);
        if(b){
            log.info("获取告警数据任务成功");
                // 30分钟 = 30 * 60秒 * 1000毫秒
                Thread.sleep(30 * 60 * 1000);
                //Thread.sleep(1000);
                log.info("线程已唤醒，继续执行后续逻辑");
            }
            Boolean aBoolean = alarmRecordResultService.updateAlarmData(recordResult.getId()+1, alarmData2.getID());
            if(aBoolean){
                log.info("告警数据修改成功");
                resultService.aggregate(results);
            }else {
                log.error("告警数据修改失败");
            }
        } catch (InterruptedException e) {
            log.error("定时任务出错{}",e);
        }
    }

//    @Async("taskExecutor")
//    @Scheduled(cron = "0 55 23 * * ?") //每23点55分执行一次
//    public void pushResultTask() {
//        log.info("====执行推送数据任务===");
//        Date date = new Date();
//        Date startTime = DateUtils.addDays(date,0);
//        Date endTime = DateUtils.addDays(date,1);
//        resultService.pushResult(startTime,endTime);
//    }
}
