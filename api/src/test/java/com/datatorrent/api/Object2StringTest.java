/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.api;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * This tests the Object2String codec
 */
public class Object2StringTest
{
  private StringCodec<TestBean> bean2String;

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

      TestBean testBean = (TestBean)o;

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
      result = 31 * result + (int)(longVal ^ (longVal >>> 32));
      return result;
    }
  }

  @Before
  public void setup()
  {
    bean2String = StringCodec.Object2String.getInstance();
    assertTrue(bean2String instanceof StringCodec.Object2String);
  }

  @Test
  public void testBeanCodecWithoutConstructorWithoutProperty()
  {
    String bean = TestBean.class.getName();
    TestBean obj = bean2String.fromString(bean);
    assertEquals("validating the bean",obj,new TestBean());
  }

  @Test
  public void testBeanCodecWithConstructorSet()
  {
    String bean = TestBean.class.getName() + ":testVal";
    TestBean obj = bean2String.fromString(bean);
    assertEquals("validating the bean", obj, new TestBean("testVal"));
  }

  @Test
  public void testBeanCodecWithConstructorPropertySet()
  {
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
    String bean = TestBean.class.getName() + ":testVal:";
    TestBean obj = bean2String.fromString(bean);
    assertEquals("validating the bean",obj,new TestBean("testVal"));
  }

  @Test
  public void testBeanCodecOnlyEmptyConstructor()
  {
    String bean = TestBean.class.getName() + ":";
    TestBean obj = bean2String.fromString(bean);
    assertEquals("validating the bean",obj,new TestBean());
  }

  @Test
  public void testBeanCodecOnlyConstructor()
  {
    String bean = TestBean.class.getName() + ": ";
    TestBean obj = bean2String.fromString(bean);
    assertEquals("validating the bean",obj,new TestBean(" "));
  }

  @Test
  public void testBeanCodecEmptyConstructorEmptyProperty()
  {
    String bean = TestBean.class.getName() + "::";
    TestBean obj = bean2String.fromString(bean);
    assertEquals("validating the bean",obj,new TestBean());
  }

  @Test
  public void testBeanCodecWithProperty()
  {
    String bean = TestBean.class.getName() + "::intVal=1";
    TestBean obj = bean2String.fromString(bean);
    TestBean expectedBean = new TestBean("");
    expectedBean.intVal = 1;
    assertEquals("validating the bean", obj, expectedBean);
  }

  @Test
  public void testBeanCodecWithAllProperties()
  {
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
    String bean = TestBean.class.getName() + "1::intVal=1";
    try {
      bean2String.fromString(bean);
      assertFalse(true);
    } catch (RuntimeException e) {
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
      return ((String)o).matches(regex);

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
