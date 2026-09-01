package com.mobile.safe.db;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.util.Date;

/**
 * 告警记录实体类
 */
@Data
@TableName("alarm_record_result_test")
public class AlarmRecordResult2 {
    /**
     * 自增主键，唯一标识每条告警记录
     */
    private Long id;

    /**
     * 告警发生的时间
     */
    private Date alarmTime;

    /**
     * 告警名称/类型(如: SQL注入、XSS攻击等)
     */
    private String alarmName;

    private String protocol;

    /**
     * 攻击源IP地址(支持IPv6)
     */
    private String srcIp;

    /**
     * 攻击源端口(可选)
     */
    private Integer attackPort;

    /**
     * 被攻击的目标IP地址
     */
    private String dstIp;

    /**
     * 攻击载荷/内容详情
     */
    private String payload;


    private String isSafeAttack;

    private String attackType;

    private String attackField;

    private String isSuccessful;

    private Integer extractInfoPromptTokens;
    private Integer extractInfoCompletionTokens;
    private Integer extractInfoTotalTokens;
    private Integer classifyMultiPromptTokens;
    private Integer classifyMultiCompletionTokens;
    private Integer classifyMultiTotalTokens;
    private Integer classifyBinaryPromptTokens;
    private Integer classifyBinaryCompletionTokens;
    private Integer classifyBinaryTotalTokens;

    private Long extractInfoUseTime;
    private Long classifyMultiUseTime;
    private Long classifyBinaryUseTime;

}