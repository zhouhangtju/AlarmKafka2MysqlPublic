package com.mobile.safe.vo;

import com.mobile.safe.db.AlarmRecordResult;
import lombok.Data;

import java.util.Date;
import java.util.List;

@Data
public class GroupResultVo {


    private String alarmTime;

    private String alarmName;

    private String srcIp;

    private Integer attackPort;


    private String dstIp;


    private Integer truePayload;

    private String payload;

    private String isSafeAttack;

    private String attackType;

    private String attackField;

    private String ids;

    private Integer id;

    private Boolean expanded;

    private List<Integer> expandedRowKeys;

    private List<Children> childrenData;
}
