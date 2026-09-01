package com.mobile.safe.db;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

@Data
@TableName("test")
public class Test {

    @TableField("SRC_IP")
    private String SRCIP;
    @TableField("ALARM_NAME")
    private String alarmName;

    @TableId(value = "ID",type = IdType.AUTO)
    @TableField("ID")
    private Long ID;

    @TableField("KEYWORD")
    private String keyword;
    @TableField("AI_TAG")
    private String aiTag;

    @TableField("PAYLOAD")
    private String payload;
    @TableField("SRC_PORT")
    private String srcPort;

}
