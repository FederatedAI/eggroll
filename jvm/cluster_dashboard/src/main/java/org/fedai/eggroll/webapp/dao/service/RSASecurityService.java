package org.fedai.eggroll.webapp.dao.service;


import org.fedai.eggroll.webapp.utils.RSAUtils;

public class RSASecurityService implements SecurityService {

    private Long keyRefreshTime = 300000L;
    private Long lastRefreshTime;
    private RSAUtils.RSAKeyPair keyPair;

    @Override
    public String getEncryptKey() throws Exception {
        return getPublicKey();
    }

    @Override
    public boolean compareValue(String passwordCipher, String realValue) throws Exception {
        return realValue.equals(RSAUtils.decrypt(passwordCipher, getPrivateKey()));
    }

    private String getPrivateKey() throws Exception {
        long currentTime = System.currentTimeMillis();
        if (lastRefreshTime == null || (currentTime - lastRefreshTime) > keyRefreshTime) {
            keyPair = RSAUtils.getKeyPair();
            lastRefreshTime = currentTime;
        }
        return keyPair.getPrivateKey();
//        return MetaInfo.ENCRYPT_PRIVATE_KEY;
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