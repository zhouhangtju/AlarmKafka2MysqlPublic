package com.mobile.safe.common;

import com.mobile.safe.db.AlarmRecordResult;
import lombok.Data;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Data
public class AggregatedData {
    private String groupKey; // srcIp+dstIp+attackType的组合
    private Date startTime;
    private Date endTime;

    private Integer truePayload;

    private Integer size;
    private List<AlarmRecordResult> results = new ArrayList<>();

    // 添加告警到聚合组
    public void addAlarm(AlarmRecordResult result) {
        this.results.add(result);
    }
}
