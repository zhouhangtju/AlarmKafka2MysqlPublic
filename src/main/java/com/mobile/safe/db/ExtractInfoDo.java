package com.mobile.safe.db;

import lombok.Data;

@Data
public class ExtractInfoDo {
    private String model;
    private Long created;
    private ResponseData response;
    private String finish_reason;
    private TokenUsage usage;
    private long use_time;

    @Data
    public static class ResponseData {
        private String result;
    }

    @Data
    public static class TokenUsage {
        private Integer prompt_tokens;
        private Integer completion_tokens;
        private Integer total_tokens;
    }
}
