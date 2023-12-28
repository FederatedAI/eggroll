package org.fedai.eggroll.core.grpc;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;


@Target({METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited

public @interface URI {
    String value();
}




