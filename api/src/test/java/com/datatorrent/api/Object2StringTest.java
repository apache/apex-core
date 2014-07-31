/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */

package com.datatorrent.api;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * This tests the Object2String codec
 */
public class Object2StringTest
{
  public static class TestBean
  {
    private int intVal;
    private String stringVal;
    private long longVal;

    public TestBean()
    {
    }

    public TestBean(String string)
    {
      intVal = -1;
      stringVal = "constructor";
      longVal = -1;
      if (string == null || string.isEmpty()) {
        return;
      }
      stringVal = string;
    }

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

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TestBean testBean = (TestBean) o;

      if (intVal != testBean.intVal) {
        return false;
      }
      if (longVal != testBean.longVal) {
        return false;
      }
      if (stringVal != null ? !stringVal.equals(testBean.stringVal) : testBean.stringVal != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      int result = intVal;
      result = 31 * result + (stringVal != null ? stringVal.hashCode() : 0);
      result = 31 * result + (int) (longVal ^ (longVal >>> 32));
      return result;
    }
  }

  @Test
  public void testBeanCodecWithoutConstructorWithoutProperty()
  {
    StringCodec.Object2String<TestBean> bean2String = new StringCodec.Object2String<TestBean>();
    String bean = TestBean.class.getName();
    TestBean obj = bean2String.fromString(bean);
    assertEquals("validating the bean",obj,new TestBean());
  }

  @Test
  public void testBeanCodecWithConstructorSet()
  {
    StringCodec.Object2String<TestBean> bean2String = new StringCodec.Object2String<TestBean>();
    String bean = TestBean.class.getName() + ":testVal";
    TestBean obj = bean2String.fromString(bean);
    assertEquals("validating the bean", obj, new TestBean("testVal"));
  }

  @Test
  public void testBeanCodecWithConstructorPropertySet()
  {
    StringCodec.Object2String<TestBean> bean2String = new StringCodec.Object2String<TestBean>();
    String bean = TestBean.class.getName() + ":testVal:intVal=10:stringVal=strVal";
    TestBean obj = bean2String.fromString(bean);
    TestBean expectedBean = new TestBean("testVal");
    expectedBean.intVal = 10;
    expectedBean.stringVal = "strVal";
    assertEquals("validating the bean", obj, expectedBean);
  }

  @Test
  public void testBeanCodecWithConstructorSetEmptyProperties()
  {
    StringCodec.Object2String<TestBean> bean2String = new StringCodec.Object2String<TestBean>();
    String bean = TestBean.class.getName() + ":testVal:";
    TestBean obj = bean2String.fromString(bean);
    assertEquals("validating the bean",obj,new TestBean("testVal"));
  }

  @Test
  public void testBeanCodecOnlyEmptyConstructor()
  {
    StringCodec.Object2String<TestBean> bean2String = new StringCodec.Object2String<TestBean>();
    String bean = TestBean.class.getName() + ":";
    TestBean obj = bean2String.fromString(bean);
    assertEquals("validating the bean",obj,new TestBean());
  }

  @Test
  public void testBeanCodecOnlyConstructor()
  {
    StringCodec.Object2String<TestBean> bean2String = new StringCodec.Object2String<TestBean>();
    String bean = TestBean.class.getName() + ": ";
    TestBean obj = bean2String.fromString(bean);
    assertEquals("validating the bean",obj,new TestBean(" "));
  }

  @Test
  public void testBeanCodecEmptyConstructorEmptyProperty()
  {
    StringCodec.Object2String<TestBean> bean2String = new StringCodec.Object2String<TestBean>();
    String bean = TestBean.class.getName() + "::";
    TestBean obj = bean2String.fromString(bean);
    assertEquals("validating the bean",obj,new TestBean());
  }

  @Test
  public void testBeanCodecWithProperty()
  {
    StringCodec.Object2String<TestBean> bean2String = new StringCodec.Object2String<TestBean>();
    String bean = TestBean.class.getName() + "::intVal=1";
    TestBean obj = bean2String.fromString(bean);
    TestBean expectedBean = new TestBean("");
    expectedBean.intVal = 1;
    assertEquals("validating the bean", obj, expectedBean);
  }

  @Test
  public void testBeanCodecWithAllProperties()
  {
    StringCodec.Object2String<TestBean> bean2String = new StringCodec.Object2String<TestBean>();
    String bean = TestBean.class.getName() + "::intVal=1:stringVal=testStr:longVal=10";
    TestBean obj = bean2String.fromString(bean);
    TestBean expectedBean = new TestBean("testStr");
    expectedBean.intVal = 1;
    expectedBean.longVal = 10;
    assertEquals("validating the bean", obj, expectedBean);
  }

  @Test
  public void testBeanWithWrongClassName()
  {
    StringCodec.Object2String<TestBean> bean2String = new StringCodec.Object2String<TestBean>();
    String bean = TestBean.class.getName() + "1::intVal=1";
    try {
      bean2String.fromString(bean);
      assertFalse(true);
    }
    catch (RuntimeException e) {
      if (e.getCause() instanceof ClassNotFoundException) {
        String expRegex = "java.lang.ClassNotFoundException: com.datatorrent.api.Object2StringTest\\$TestBean1";
        assertThat("exception message", e.getMessage(), RegexMatcher.matches(expRegex));
        return;
      }
      throw e;
    }
  }

  @Test
  public void testBeanFailure()
  {
    StringCodec.Object2String<TestBean> bean2String = new StringCodec.Object2String<TestBean>();
    String bean = TestBean.class.getName() + "::intVal=1:stringVal=hello:longVal=10";
    TestBean obj = bean2String.fromString(bean);
    TestBean expectedBean = new TestBean("hello");
    expectedBean.intVal = 1;
    expectedBean.longVal = 10;
    assertEquals("validating the bean", obj, expectedBean);
  }

  public static class RegexMatcher extends BaseMatcher<String>
  {
    private final String regex;

    public RegexMatcher(String regex)
    {
      this.regex = regex;
    }

    @Override
    public boolean matches(Object o)
    {
      return ((String) o).matches(regex);

    }

    @Override
    public void describeTo(Description description)
    {
      description.appendText("matches regex=" + regex);
    }

    public static RegexMatcher matches(String regex)
    {
      return new RegexMatcher(regex);
    }
  }

}
