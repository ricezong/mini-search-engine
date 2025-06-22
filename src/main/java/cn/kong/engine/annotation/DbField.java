package cn.kong.engine.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface DbField {

    String name() default "";

    String type() default "";

    boolean primaryKey() default false;

    boolean autoIncrement() default false;

    boolean nullable() default true;

    String defaultValue() default "";
}