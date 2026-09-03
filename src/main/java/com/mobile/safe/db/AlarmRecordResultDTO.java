package com.mobile.safe.db;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

import java.util.Date;

@Data
public class AlarmRecordResultDTO {

//    /**
//     * 自增主键，唯一标识每条告警记录
//     */
//    private Long id;

    /**
     * 告警发生的时间
     */
    private Date alarmTime;

    /**
     * 告警名称/类型(如: SQL注入、XSS攻击等)
     */
    private String alarmName;

    @JSONField(name = "APP_PROTOCOL")
    private String appProtocol;

    @JSONField(name = "DEVICE_TYPE")
    private String deviceType;

    @JSONField(name = "PROTOCOL")
    private String protocol;

    /**
     * 攻击源IP地址(支持IPv6)
     */
    @JSONField(name = "SRC_IP")
    private String srcIp;

    /**
     * 攻击源端口(可选)
     */
    private Integer attackPort;

    /**
     * 被攻击的目标IP地址
     */
    @JSONField(name = "DST_IP")
    private String dstIp;

    /**
     * 攻击载荷/内容详情
     */
    @JSONField(name = "PAYLOAD")
    private String payload;

    @JSONField(name = "REQUEST_PAYLOAD")
    private  String requestPayload;

    @JSONField(name = "REQUEST_MESSAGE")
    private  String requestMessage;


    private String isSafeAttack;

    private String attackType;

    private String attackField;

    private String isSuccessful;

    @JSONField(name = "RESPONSE_CODE")
    private String responseCode;

    @JSONField(name = "RESPONSE_MESSAGE")
    private String responseMessage;

    @JSONField(name = "CREATE_TIME", format = "yyyy-MM-dd HH:mm:ss.SSS")
    private Date createTime;

    private Integer classifyBinaryPromptTokens;
    private Integer classifyBinaryCompletionTokens;
    private Integer classifyBinaryTotalTokens;
    private Integer classifyMultiPromptTokens;
    private Integer classifyMultiCompletionTokens;
    private Integer classifyMultiTotalTokens;
    private Integer extractInfoPromptTokens;
    private Integer extractInfoCompletionTokens;
    private Integer extractInfoTotalTokens;



    private Long extractInfoUseTime;
    private Long classifyMultiUseTime;
    private Long classifyBinaryUseTime;
}
