package com.datatorrent.api.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Annotation to specify characteristics of an application.<p>
 *
 * @since 1.0.1
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ApplicationAnnotation {
  
  /**
   * <p>Compile time application alias.</p> 
   * <p>There are 3 ways to specify application name</p>
   * <li>Compile time application alias -- specified in annotation</li>
   * <li>Configuration time application alias -- specified in dt-site.xml</li>
   * <li>Runtime application alias -- specified in application code</li>
   * 
   */
  public String name();
  
}
