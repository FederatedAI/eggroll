package com.webank.eggroll.clustermanager.command;

import java.util.Arrays;

public class ErCommandResponse {
    private String id;
    private ErCommandRequest request;
    private byte[][] results;

    public ErCommandResponse(String id) {
        this.id = id;
        this.request = null;
        this.results = null;
    }

    public ErCommandResponse(String id, ErCommandRequest request, byte[][] results) {
        this.id = id;
        this.request = request;
        this.results = results;
    }

    public String getId() {
        return id;
    }

    public ErCommandRequest getRequest() {
        return request;
    }

    public void setRequest(ErCommandRequest request) {
        this.request = request;
    }

    public byte[][] getResults() {
        return results;
    }

    public void setResults(byte[][] results) {
        this.results = results;
    }

    @Override
    public String toString() {
        return "ErCommandResponse{" +
                "id='" + id + '\'' +
                ", request=" + request +
                ", results=" + Arrays.toString(results) +
                '}';
    }
}