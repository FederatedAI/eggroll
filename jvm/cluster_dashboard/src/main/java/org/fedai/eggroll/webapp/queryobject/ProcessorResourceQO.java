package org.fedai.eggroll.webapp.queryobject;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProcessorResourceQO {

    private String processorId;
    private String sessionId;
    private String serverNodeId;
    private String resourceType;
    private String status;

    private Integer pageNum = 1;
    private Integer pageSize = 10;

}
