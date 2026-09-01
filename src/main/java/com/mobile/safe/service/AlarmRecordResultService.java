package com.mobile.safe.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.mobile.safe.db.AlarmRecordResult;

import java.util.List;

public interface AlarmRecordResultService extends IService<AlarmRecordResult>{


    Boolean saveAlarmData(List<AlarmRecordResult> results);

    List<AlarmRecordResult> getRecordResults(Long startID,Long endID);

    Boolean updateAlarmData(Long startID,Long endID);
}
