package com.mobile.safe.db;

import lombok.Data;

import java.util.List;

@Data
public class ModelsDo {
        private String object;
        private List<ModelData> data;

        @Data
        public static class ModelData {
            private String id;
            private String object;
            private long created;
            private String owned_by;
        }
}
