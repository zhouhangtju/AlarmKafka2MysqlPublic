package com.mobile.safe.db;

import lombok.Data;

@Data
public class ClassifyBinaryDo {
    private String model;
    private long created;
    private Response response;
    private String finish_reason;
    private Usage usage;

    private long use_time;

    @Data
    public static class Response {
        private String result;
    }

    @Data
    public static class Usage {
        private int prompt_tokens;
        private int completion_tokens;
        private int total_tokens;
    }
}
