package com.eggroll.core.constant;

public enum ProcessorType {


     EGG_PAIR("egg_pair") ,DEEPSPEED_DOWNLOAD("deepspeed_download");

    private ProcessorType(String value){
                this.value = value;
            }
    private  String value;
}
