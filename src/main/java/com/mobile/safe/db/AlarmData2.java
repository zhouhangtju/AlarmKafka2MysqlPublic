package com.mobile.safe.db;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.mobile.safe.SafetyApplication;
import lombok.Data;

import java.util.Date;

/**
 * 告警记录实体类
 */
@Data
@TableName("CK_ASSIST_AI_JUDGE_DATA")
public class AlarmData2 {
    /**
     * 自增主键，唯一标识每条告警记录
     */
    @TableId(value = "ID")
    @TableField("ID")
    private Long ID;
    /**
     * 告警名称/类型(如: SQL注入、XSS攻击等)
     */
    @TableField("EVENT_NAME")
    private String alarmName;
    /**
     * 攻击源IP地址(支持IPv6)
     */
    @TableField("SRC_IP")
    private String srcIp;

    /**
     * 被攻击的目标IP地址
     */
    @TableField("DST_IP")
    private String dstIp;

    /**
     * 攻击源端口(可选)
     */
    @TableField("PROTOCOL")
    private String protocol;
    @TableField("REQUEST_MESSAGE")
    private String payload;

    @TableField("RESPONSE_CODE")
    private String responseCode;

    @TableField("RESPONSE_MESSAGE")
    private String responseMessage;
    @TableField("AI_CLASSIFYBINARY_RESULT")
    private String aiTag;
    @TableField("AI_EXTRACTINFO_RESULT")
    private String keyword;
    @TableField("AI_CLASSIFYMULTI_RESULT")
    private String aiAttackType;
    @TableField("CREATE_TIME")
    private Date lastAlarmTime;

    @TableField("SRC_PORT")
    private String srcPort;

    @TableField("PROMPT_TOKENS1")
    private String promptTokens1;

    @TableField("COMPLETION_TOKENS1")
    private String completionTokens1;
    @TableField("TOTAL_TOKENS1")
    private String totalTokens1;

    @TableField("PROMPT_TOKENS2")
    private String promptTokens2;

    @TableField("COMPLETION_TOKENS2")
    private String completionTokens2;
    @TableField("TOTAL_TOKENS2")
    private String totalTokens2;

    @TableField("PROMPT_TOKENS3")
    private String promptTokens3;

    @TableField("COMPLETION_TOKENS3")
    private String completionTokens3;
    @TableField("TOTAL_TOKENS3")
    private String totalTokens3;
}