package com.eggroll.core.pojo;

import lombok.Data;


@Data
public class IndexContentTemp {
    private int index;
    private ContainerContent containerContent;


    public IndexContentTemp(int index,ContainerContent containerContent) {
        this.index = index;
        this.containerContent = containerContent;

    }



}
