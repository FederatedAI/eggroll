package org.fedai.eggroll.webapp.queryobject;


import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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


    private String createdAt;

    //查询前多少条数
    private Integer topCount;

    private Integer pageNum = 1;
    private Integer pageSize = 10;


}
