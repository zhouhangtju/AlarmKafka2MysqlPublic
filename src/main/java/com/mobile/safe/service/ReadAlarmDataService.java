package com.mobile.safe.service;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.mobile.safe.db.AlarmData2;
import com.mobile.safe.db.AlarmRecordResult;

import java.util.List;


public interface ReadAlarmDataService {
    public List<AlarmRecordResult>  readAlarmData(Long startID,Long endID);

    AlarmRecordResult getNewData();

    AlarmRecordResult getOldData();

    AlarmRecordResult importData();
}
