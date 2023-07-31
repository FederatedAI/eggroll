//package com.webank.eggroll.core.util;
//
//import com.webank.eggroll.core.constant.StringConstants;
//
//public class CommandUtils {
//    public static String toServiceName(String prefix, String methodName, String delim) {
//        return prefix + delim + methodName;
//    }
//
//    public static CommandURI toCommandURI(String prefix, String methodName, String delim) {
//        String serviceName = toServiceName(prefix, methodName, delim);
//        return new CommandURI(serviceName);
//    }
//
//    public static CommandURI toCommandURI(String prefix, String methodName) {
//        return toCommandURI(prefix, methodName, StringConstants.SLASH());
//    }
//}