package com.mobile.safe.dto;

import lombok.Data;

@Data
public class TokenRequest {
    private String username;

    private String password;

    private String grant_type;

    private String client_id;

    private String client_secret;
}
