package org.fedai.eggroll.core.pojo;

import lombok.Data;

import java.util.List;

@Data
public class IndexContentsTemp {
    private List<Integer> indexes;
    private List<ContainerContent> containerContents;


    public IndexContentsTemp(List<Integer> indexes, List<ContainerContent> containerContents) {
        this.indexes = indexes;
        this.containerContents = containerContents;

    }


}
