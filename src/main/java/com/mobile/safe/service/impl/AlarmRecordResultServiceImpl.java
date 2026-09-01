package com.mobile.safe.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.mobile.safe.dao.AlarmRecordResultDao;
import com.mobile.safe.db.AlarmRecordResult;
import com.mobile.safe.service.AlarmRecordResultService;
import com.mobile.safe.service.ReadAlarmDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class AlarmRecordResultServiceImpl extends ServiceImpl<AlarmRecordResultDao, AlarmRecordResult> implements AlarmRecordResultService{

    @Autowired
    private ReadAlarmDataService readAlarmDataService;


    @Override
    @Transactional(rollbackFor = Exception.class)
    public Boolean saveAlarmData(List<AlarmRecordResult> results) {
        boolean b = this.saveBatch(results,500);
        return b;
    }

    @Override
    public List<AlarmRecordResult> getRecordResults(Long startID, Long endID) {
        List<AlarmRecordResult> results = readAlarmDataService.readAlarmData(startID,endID);
        return results;
    }

    @Override
    public Boolean updateAlarmData(Long startID, Long endID) {
        List<AlarmRecordResult> results = readAlarmDataService.readAlarmData(startID,endID);
        boolean b = this.updateBatchById(results);
        return b;
    }


}
