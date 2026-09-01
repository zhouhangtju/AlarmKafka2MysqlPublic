package com.mobile.safe.dto;

import lombok.Data;

@Data
public class PushResult {
    private Integer handleAlarmNum;
    private Integer classifyBinaryNum;
    private Integer classifyMultiNum;
    private Integer extractInfoNum;
    private String safeAttackRatio;
    private String classifyBinaryTimeAvg;
    private String classifyBinaryPromptTokensAvg;
    private String classifyBinaryCompletionTokensAvg;
    private String classifyMultiTimeAvg;
    private String classifyMultiPromptTokensAvg;
    private String classifyMultiCompletionTokensAvg;
    private String extractInfoTimeAvg;
    private String extractInfoPromptTokensAvg;
    private String extractInfoCompletionTokensAvg;
    private String classifyBinaryTotalTokensAvg;
    private String classifyMultiTotalTokensAvg;
    private String extractInfoTotalTokensAvg;
}
