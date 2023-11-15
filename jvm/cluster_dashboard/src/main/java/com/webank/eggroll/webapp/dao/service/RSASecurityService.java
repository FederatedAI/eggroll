package com.webank.eggroll.webapp.dao.service;

import com.webank.eggroll.webapp.utils.RSAUtils;
import lombok.Value;

public class RSASecurityService implements SecurityService {

    private Long keyRefreshTime = 300000L;
    private Long lastRefreshTime;
    private RSAUtils.RSAKeyPair keyPair;

    @Override
    public String getEncryptKey() throws Exception {
        return getPublicKey();
    }

    @Override
    public boolean compareValue(String encryptedValue, String realValue) throws Exception {
        return realValue.equals(RSAUtils.decrypt(encryptedValue, getPrivateKey()));
    }

    private String getPrivateKey() throws Exception {
        long currentTime = System.currentTimeMillis();
        if (lastRefreshTime == null || (currentTime - lastRefreshTime) > keyRefreshTime) {
            keyPair = RSAUtils.getKeyPair();
            lastRefreshTime = currentTime;
        }
        return keyPair.getPrivateKey();
    }

    private String getPublicKey() throws Exception {
        long currentTime = System.currentTimeMillis();
        if (lastRefreshTime == null || (currentTime - lastRefreshTime) > keyRefreshTime) {
            keyPair = RSAUtils.getKeyPair();
            lastRefreshTime = currentTime;
        }
        return keyPair.getPublicKey();
    }
}