package com.mobile.safe.dto;

import lombok.Data;

@Data
public class IndicatorRequest {
    private String agentName;
    private Indicator indicator;
    private String reportTime;

    @Data
    public static class Indicator{
        private Integer handleAlarmNum;
        private Integer classifyBinaryNum;
        private Integer classifyMultiNum;
        private Integer extractInfoNum;
        private String classifyBinaryTimeAvg;
        private String classifyMultiTimeAvg;
        private String extractInfoTimeAvg;
        private String classifyBinaryTotalTokensAvg;
        private String classifyMultiTotalTokensAvg;
        private String extractInfoTotalTokensAvg;
    }
}
