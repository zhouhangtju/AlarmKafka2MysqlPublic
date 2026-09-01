package com.mobile.safe.dto;

import lombok.Data;

@Data
public class AlarmRecordResultKafka {
    private Long id;

    private String payload;
}
