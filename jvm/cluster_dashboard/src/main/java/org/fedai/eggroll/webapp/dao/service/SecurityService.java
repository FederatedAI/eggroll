package org.fedai.eggroll.webapp.dao.service;

public interface SecurityService {

    String getEncryptKey() throws Exception;

    boolean compareValue(String passwordCipher, String passwordValue) throws Exception;

}