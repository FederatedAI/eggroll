package com.webank.eggroll.rollframe.pytorch;

/**
 * a interface about matrix data to input jni.
 */
public class TorchTensor {
    private long address = -1;
    private long size = -1;

    // if FrameBatch can add more variables,such as col, it better.
    public void setAddress(long address) {
        this.address = address;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getAddress() {
        return address;
    }

    public long getSize() {
        return size;
    }
}


