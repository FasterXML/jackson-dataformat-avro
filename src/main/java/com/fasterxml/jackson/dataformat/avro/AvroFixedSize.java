package com.fasterxml.jackson.dataformat.avro;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface AvroFixedSize {
    String name();
    String namespace() default "";
    int size();
}
