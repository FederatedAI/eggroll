package org.fedai.eggroll.webapp.entity;




import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;

import javax.validation.constraints.NotNull;
import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class UserDTO implements Serializable {
    @NotNull(message = "username can't be null!")
    private String username;

    @NotNull(message = "password can't be null!")
    private String password;

}
