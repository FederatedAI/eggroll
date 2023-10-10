package com.webank.eggroll.webapp.queryobject;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class SessionMainQO {

    private String sessionId;
    private String name;
    private String status;
    private String tag;

    private Integer pageNum = 1;
    private Integer pageSize = 10;

}
