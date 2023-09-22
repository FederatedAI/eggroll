package com.webank.eggroll.webapp.entity;

import lombok.Data;

@Data
public class UserCredentials {
    private String username;
    private String password;

    public UserCredentials(String username, String password) {
        this.username = username;
        this.password = password;
    }


}

