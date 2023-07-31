//package com.webank.eggroll.core.util;
//
//import io.grpc.Status;
//import io.grpc.StatusRuntimeException;
//import org.apache.commons.lang3.exception.ExceptionUtils;
//
//public class ErrorUtils {
//    public static StatusRuntimeException toGrpcRuntimeException(Throwable throwable) {
//        StatusRuntimeException result = null;
//
//        if (throwable instanceof StatusRuntimeException) {
//            result = (StatusRuntimeException) throwable;
//        } else {
//            StringBuilder sb = new StringBuilder();
//            // todo:0: add hop info
//
//            result = Status.INTERNAL
//                    .withCause(throwable)
//                    .withDescription(RuntimeUtils.getMySiteLocalAddressAndPort() + ": " + getStackTraceString(throwable))
//                    .asRuntimeException();
//        }
//
//        return result;
//    }
//
//    public static String getStackTraceString(Throwable throwable) {
//        return ExceptionUtils.getStackTrace(throwable);
//    }
//}