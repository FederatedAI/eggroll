package com.webank.eggroll.webapp.dao.service;

public interface SecurityService {

    String getEncryptKey() throws Exception;

    boolean compareValue(String encryptedValue, String realValue) throws Exception;

}