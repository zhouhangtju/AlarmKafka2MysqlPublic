package com.mobile.safe.vo;

import lombok.Data;

import java.util.Date;
@Data
public class Children {
    private Long id;

    /**
     * 告警发生的时间
     */
    private String alarmTime;

    /**
     * 告警名称/类型(如: SQL注入、XSS攻击等)
     */
    private String alarmName;

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

}
