package com.mobile.safe.service;

import com.mobile.safe.db.AlarmRecordResult;
import com.mobile.safe.dto.PushResponse;
import com.mobile.safe.dto.PushResult;

import java.util.Date;
import java.util.List;

public interface ResultService {
    void aggregate(List<AlarmRecordResult> results);

    void aggregateNew(List<AlarmRecordResult> results);


    void aggregateTenMin(List<AlarmRecordResult> results);

    /**
     * 获取指标信息
     * @return PushResult
     */
    PushResult getResult(Date startTime,Date endTime);

    /**
     * 获取token信息
     * @return String
     */
    String getToken();

    void pushResult(Date startTime,Date endTime);

    PushResult getResultMock();
}
