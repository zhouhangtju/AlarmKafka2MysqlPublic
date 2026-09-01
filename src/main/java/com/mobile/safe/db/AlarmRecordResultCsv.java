package com.mobile.safe.db;

import com.mobile.safe.convert.DateConverter;
import com.opencsv.bean.CsvBindByPosition;
import com.opencsv.bean.CsvCustomBindByPosition;
import lombok.Data;

import java.util.Date;

/**
 * 告警记录实体类
 */
@Data
public class AlarmRecordResultCsv {
    /**
     * 自增主键，唯一标识每条告警记录
     */
    private Long id;

    /**
     * 告警发生的时间
     */
    @CsvCustomBindByPosition(position = 1,converter = DateConverter.class)
    private Date alarmTime;

    /**
     * 被攻击的目标IP地址
     */
    @CsvBindByPosition(position = 2)
    private String dstIp;

    /**
     * 攻击源IP地址(支持IPv6)
     */
    @CsvBindByPosition(position = 3)
    private String srcIp;
    /**
     * 告警名称/类型(如: SQL注入、XSS攻击等)
     */
    @CsvBindByPosition(position = 4)
    private String alarmName;

    /**
     * 攻击载荷/内容详情
     */
    @CsvBindByPosition(position = 5)
    private String payload;
    @CsvBindByPosition(position = 6)
    private String isSafeAttack;

    @CsvBindByPosition(position = 7)
    private String attackType;
    @CsvBindByPosition(position = 8)
    private String attackField;
    /**
     * 攻击源端口(可选)
     */
    @CsvBindByPosition(position = 9)
    private Integer attackPort;
}