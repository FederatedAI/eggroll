//package org.fedai.core.pojo;
//
//import org.fedai.core.config.Dict;
//import org.fedai.core.config.ErConf;
//import org.fedai.core.config.MetaInfo;
//
//import java.util.Map;
//import java.util.Properties;
//
//@SuppressWarnings("unused")
//public class RuntimeErConf extends ErConf {
//    public RuntimeErConf(Properties prop) {
//        super();
//        ErConf.getConf().putAll(prop);
//    }
//
//    public RuntimeErConf(ErSessionMeta sessionMeta) {
//        super();
//        for (Map.Entry<String, String> entry : sessionMeta.getOptions().entrySet()) {
//            ErConf.getConf().put(entry.getKey(), entry.getValue());
//        }
//        ErConf.getConf().put(Dict.CONFKEY_SESSION_ID, sessionMeta.getId());
//        ErConf.getConf().put(Dict.CONFKEY_SESSION_NAME, sessionMeta.getName());
//    }
//
//    public RuntimeErConf(StartContainersRequest startContainersRequest) {
//        super();
//        for (Map.Entry<String, String> entry : startContainersRequest.options.entrySet()) {
//            ErConf.getConf().put(entry.getKey(), entry.getValue());
//        }
//        ErConf.getConf().put(Dict.CONFKEY_SESSION_ID, startContainersRequest.sessionId);
//        ErConf.getConf().put(Dict.CONFKEY_SESSION_NAME, startContainersRequest.name);
//    }
//
//    public RuntimeErConf(Map<String, String> conf) {
//        super();
//        ErConf.getConf().putAll(conf);
//    }
//
////    public static int getPort() {
////        return StaticErConf.getPort();
////    }
////
////    public static String getModuleName() {
////        return StaticErConf.getModuleName();
////    }
//}