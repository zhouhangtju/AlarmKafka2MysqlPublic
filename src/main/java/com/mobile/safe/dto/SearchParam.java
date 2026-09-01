package com.mobile.safe.dto;

import lombok.Data;

@Data
public class SearchParam {
    private String startTime;

    private String endTime;

    private int page;

    private int pageSize;
}
