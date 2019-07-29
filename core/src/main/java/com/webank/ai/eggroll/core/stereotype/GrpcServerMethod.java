package com.webank.ai.eggroll.core.stereotype;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface GrpcServerMethod {
    public boolean hasReturnValue() default false;
}
