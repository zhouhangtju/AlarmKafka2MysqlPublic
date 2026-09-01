package com.mobile.safe.db;

import lombok.Data;
import java.util.Date;

/**
 * 告警记录实体类
 */
@Data
public class AlarmRecord {
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

    /**
     * 攻击源IP地址(支持IPv6)
     */
    private String attackIp;

    /**
     * 攻击源端口(可选)
     */
    private Integer attackPort;

    /**
     * 被攻击的目标IP地址
     */
    private String targetIp;

    /**
     * 攻击载荷/内容详情
     */
    private String payload;

}