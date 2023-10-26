package com.webank.eggroll.webapp.queryobject;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class NodeDetailQO {

    private Integer nodeNum;
    private String sessionId;

    private Integer pageNum = 1;
    private Integer pageSize = 10;

}
