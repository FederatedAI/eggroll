package com.webank.eggroll.webapp.entity;


import lombok.Data;

import java.io.Serializable;

@Data
public class UserInfo implements Serializable {

    private String username;
    private String password;

    public UserInfo(String username, String password) {
        this.username = username;
        this.password = password;
    }
}
