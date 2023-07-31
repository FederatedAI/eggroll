package com.webank.eggroll.clustermanager.command;

import java.util.Arrays;
import java.util.Map;

public class ErCommandRequest {
    private String id;
    private String uri;
    private byte[][] args;
    private Map<String, byte[]> kwargs;

    public ErCommandRequest(String uri) {
        this.id = String.valueOf(System.currentTimeMillis());
        this.uri = uri;
        this.args = null;
        this.kwargs = null;
    }

    public ErCommandRequest(String id, String uri, byte[][] args, Map<String, byte[]> kwargs) {
        this.id = id;
        this.uri = uri;
        this.args = args;
        this.kwargs = kwargs;
    }

    public String getId() {
        return id;
    }

    public String getUri() {
        return uri;
    }

    public byte[][] getArgs() {
        return args;
    }

    public void setArgs(byte[][] args) {
        this.args = args;
    }

    public Map<String, byte[]> getKwargs() {
        return kwargs;
    }

    public void setKwargs(Map<String, byte[]> kwargs) {
        this.kwargs = kwargs;
    }

    @Override
    public String toString() {
        return "ErCommandRequest{" +
                "id='" + id + '\'' +
                ", uri='" + uri + '\'' +
                ", args=" + Arrays.toString(args) +
                ", kwargs=" + kwargs +
                '}';
    }
}