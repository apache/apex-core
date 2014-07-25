package com.datatorrent.api;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;
/**
 * Created by gaurav on 7/24/14.
 */
public class Bean2StringTest
{
  public static class TestBean{
    private int intVal;
    private String stringVal;
    private long longVal;

    public int getIntVal()
    {
      return intVal;
    }

    public void setIntVal(int intVal)
    {
      this.intVal = intVal;
    }

    public String getStringVal()
    {
      return stringVal;
    }

    public void setStringVal(String stringVal)
    {
      this.stringVal = stringVal;
    }

    public long getLongVal()
    {
      return longVal;
    }

    public void setLongVal(long longVal)
    {
      this.longVal = longVal;
    }

    @Override
    public String toString()
    {
      return "TestBean{" +
        "intVal=" + intVal +
        ", stringVal='" + stringVal + '\'' +
        ", longVal=" + longVal +
        '}';
    }
  }

  @Test
  public void getBeanCodec(){
    Bean2String<TestBean> bean2String = new Bean2String<TestBean>();
    String bean = TestBean.class.getName()+":intVal=1";
    TestBean obj = bean2String.fromString(bean);
    logger.debug("Bean Object {}",obj.toString());
    assertEquals("Validating bean Object","TestBean{intVal=1, stringVal='null', longVal=0}",obj.toString());
  }

  @Test
  public void getFullBeanObject(){
    Bean2String<TestBean> bean2String = new Bean2String<TestBean>();
    String bean = TestBean.class.getName()+":intVal=1:stringVal=hello:longVal=10";
    TestBean obj = bean2String.fromString(bean);
    logger.debug("Bean Object {}",obj.toString());
    assertEquals("Validating bean Object","TestBean{intVal=1, stringVal='hello', longVal=10}",obj.toString());
  }

  @Test
  public void getBeanCodecNegativeTest(){
    Bean2String<TestBean> bean2String = new Bean2String<TestBean>();
    String bean = TestBean.class.getName()+"1:intVal=1";
    try {
      TestBean obj = bean2String.fromString(bean);
    }catch(Throwable ex){
      logger.debug("Caught class not found exception",ex.getCause());
    }
  }

  @Test
  public void getBeanNegativeValidation(){
    Bean2String<TestBean> bean2String = new Bean2String<TestBean>();
    String bean = TestBean.class.getName()+":intVal=1:stringVal=hello:longVal=10";
    TestBean obj = bean2String.fromString(bean);
    logger.debug("Bean Object {}",obj.toString());
    try {
      assertEquals("Validating bean Object", "TestBean{intVal=10, stringVal='hello', longVal=10}", obj.toString());
    }catch(Throwable ex){
      logger.debug("Caught validation exception",ex);
    }

  }
  private static final Logger logger = LoggerFactory.getLogger(Bean2StringTest.class);

}
