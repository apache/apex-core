/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.moduleexperiment;

import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import junit.framework.Assert;

import org.junit.Test;

public class ValidationTest {

  public class MyBean{
    @NotNull
    @Pattern(regexp=".*malhar.*", message="Value has to contain 'malhar'!")
    String x;

    @Min(2)
    int y;
  }

  @Test
  public void testValidator() {

    MyBean bean = new MyBean();
    bean.x = "malharxxx";
    bean.y = 1;

    ValidatorFactory factory =
        Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();
    Set<ConstraintViolation<MyBean>> constraintViolations =
             validator.validate(bean);
    for (ConstraintViolation<MyBean> cv : constraintViolations) {
      System.out.println("validation error: " + cv);
    }
    Assert.assertEquals("",1, constraintViolations.size());
    ConstraintViolation<MyBean> cv = constraintViolations.iterator().next();
    Assert.assertEquals("", bean.y, cv.getInvalidValue());
    Assert.assertEquals("", "y", cv.getPropertyPath().toString());
  }

}
