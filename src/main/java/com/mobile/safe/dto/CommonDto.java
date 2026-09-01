package com.mobile.safe.dto;

import lombok.Data;

import java.util.List;

@Data
public class CommonDto {
    private List<String> content;

    private String adapter;
}
