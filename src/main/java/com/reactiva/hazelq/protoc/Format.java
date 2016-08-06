package com.reactiva.hazelq.protoc;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Format {

	int offset();
	int length();
	Attribute attribute();
	String constant() default "";
	boolean dateField() default false;
	String dateFormat() default "";
	boolean strictSetter() default false;
}
