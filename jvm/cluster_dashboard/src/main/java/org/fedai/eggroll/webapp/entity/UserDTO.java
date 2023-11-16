package org.fedai.eggroll.webapp.entity;




import lombok.Data;

import javax.validation.constraints.NotNull;
import java.io.Serializable;

@Data
public class UserDTO implements Serializable {
    @NotNull(message = "username can't be null!")
    private String username;

    @NotNull(message = "password can't be null!")
    private String password;

    public UserDTO(String username, String password) {
        this.username = username;
        this.password = password;
    }
}
