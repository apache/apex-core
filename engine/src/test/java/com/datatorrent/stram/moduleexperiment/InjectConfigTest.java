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
package com.datatorrent.stram.moduleexperiment;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.ConvertUtilsBean;
import org.apache.commons.beanutils.Converter;
import org.apache.commons.beanutils.PropertyUtilsBean;
import org.apache.hadoop.conf.Configuration;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.MembersInjector;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;

public class InjectConfigTest
{

  private static Logger LOG = LoggerFactory.getLogger(InjectConfigTest.class);

  public class MyBean
  {
    @NotNull
    @Pattern(regexp = ".*malhar.*", message = "Value has to contain 'malhar'!")
    String x;

    @Min(2)
    int y;

    @InjectConfig(key = "stringKey")
    private String stringField;

    @InjectConfig(key = "urlKey")
    private java.net.URL urlField;

    @InjectConfig(key = "stringArrayKey")
    private String[] stringArrayField;

  }

  public class TestGuiceModule extends AbstractModule
  {
    final Configuration conf;

    public TestGuiceModule(Configuration conf)
    {
      this.conf = conf;
    }

    private final ConvertUtilsBean converters = new ConvertUtilsBean();

    /**
     * Finds all configuration injection points for given class and subclasses.
     * Determines injection points prior to processing configuration for
     * deferred dynamic lookup. This is the reverse of using @Inject, where
     * configuration lookup and binding would need to occur before knowing what
     * is required...
     */
    private class ConfigurableListener implements TypeListener
    {
      @Override
      public <T> void hear(TypeLiteral<T> typeLiteral, TypeEncounter<T> typeEncounter)
      {
        for (Class<?> c = typeLiteral.getRawType(); c != Object.class; c = c.getSuperclass()) {
          LOG.debug("Inspecting fields for " + c);
          for (Field field : c.getDeclaredFields()) {
            if (field.isAnnotationPresent(InjectConfig.class)) {
              typeEncounter.register(new ConfigurationInjector<T>(field, field.getAnnotation(InjectConfig.class)));
            }
          }
        }
      }
    }

    /**
     * Process configuration for given field and annotation instance.
     * @param <T>
     */
    private class ConfigurationInjector<T> implements MembersInjector<T>
    {
      private final Field field;
      private final InjectConfig annotation;

      ConfigurationInjector(Field field, InjectConfig annotation)
      {
        this.field = field;
        this.annotation = annotation;
        field.setAccessible(true);
      }

      @Override
      public void injectMembers(T t)
      {
        try {
          LOG.debug("Processing " + annotation + " for field " + field);
          String value = conf.get(annotation.key());
          if (value == null) {
            if (annotation.optional() == false) {
              throw new IllegalArgumentException("Cannot inject " + annotation);
            }
            return;
          }
          Converter c = converters.lookup(field.getType());
          if (c == null) {
            throw new IllegalArgumentException("Cannot find a converter for: " + field);
          }
          field.set(t, c.convert(field.getType(), value));
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    protected void configure()
    {
      bindListener(Matchers.any(), new ConfigurableListener());
      bind(Configuration.class).toInstance(conf);
    }
  }

  public class MyBeanExt extends MyBean
  {
    @InjectConfig(key = "anotherStringKey")
    private String anotherInjectableField;
  }

  @Test
  public void testBinding() throws Exception
  {

    Configuration conf = new Configuration(false);
    conf.set("stringKey", "someStringValue");
    conf.set("urlKey", "http://localhost:9999");
    conf.set("stringArrayKey", "a,b,c");

    // ensure super classes are processed
    MyBean bean = new MyBeanExt();

    Injector injector = Guice.createInjector(new TestGuiceModule(conf));
    injector.injectMembers(bean);

    Assert.assertEquals("", "someStringValue", bean.stringField);
    Assert.assertEquals("", new java.net.URL(conf.get("urlKey")), bean.urlField);
    Assert.assertArrayEquals("", new String[]{"a", "b", "c"}, bean.stringArrayField);
  }

  public static class BeanUtilsTestBean
  {
    public static class NestedBean
    {
      private NestedBean(String s)
      {
        nestedProperty = s;
      }

      public NestedBean()
      {
      }

      public String nestedProperty = "nested1";
    }

    public int intProp;

    public int getIntProp()
    {
      return intProp;
    }

    public void setIntProp(int prop)
    {
      this.intProp = prop;
    }

    public NestedBean nested = new NestedBean();
    public List<NestedBean> nestedList = Arrays.asList(new NestedBean("nb1"), new NestedBean("nb2"));
    public URL url;
    public String string2;
    public transient String transientProperty = "transientProperty";

    public java.util.concurrent.ConcurrentHashMap<String, String> mapProperty = new java.util.concurrent
        .ConcurrentHashMap<>();

    public java.util.concurrent.ConcurrentHashMap<String, String> getMapProperty()
    {
      return mapProperty;
    }

    public java.util.concurrent.ConcurrentHashMap<String, String> nullMap;

  }

  @Test
  public void testBeanUtils() throws Exception
  {
    // http://www.cowtowncoder.com/blog/archives/2011/02/entry_440.html

    BeanUtilsTestBean testBean = new BeanUtilsTestBean();
    testBean.url = new URL("http://localhost:12345/context");

    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> properties = mapper.convertValue(testBean, Map.class);
    LOG.debug("testBean source: {}", properties);

    BeanUtilsTestBean testBean2 = new BeanUtilsTestBean();
    testBean2.string2 = "testBean2";
    Assert.assertFalse("contains transientProperty", properties.containsKey("transientProperty"));
    Assert.assertTrue("contains string2", properties.containsKey("string2"));
    properties.remove("string2"); // remove null
    //properties.put("string3", "");

    BeanUtilsBean bub = new BeanUtilsBean();
    try {
      bub.getProperty(testBean, "invalidProperty");
      Assert.fail("exception expected");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Unknown property 'invalidProperty'"));
    }
    bub.setProperty(properties, "mapProperty.someKey", "someValue");

    JsonNode sourceTree = mapper.convertValue(testBean2, JsonNode.class);
    JsonNode updateTree = mapper.convertValue(properties, JsonNode.class);
    merge(sourceTree, updateTree);

    //   mapper.readerForUpdating(testBean2).readValue(sourceTree);
    //   Assert.assertEquals("preserve existing value", "testBean2", testBean2.string2);
    //   Assert.assertEquals("map property", "someValue", testBean2.mapProperty.get("someKey"));

    //   LOG.debug("testBean cloned: {}", mapper.convertValue(testBean2, Map.class));

    PropertyUtilsBean propertyUtilsBean = BeanUtilsBean.getInstance().getPropertyUtils();
    //PropertyDescriptor pd = propertyUtilsBean.getPropertyDescriptor(testBean2, "mapProperty.someKey2");

    // set value on non-existing property
    try {
      propertyUtilsBean.setProperty(testBean, "nonExistingProperty.someProperty", "ddd");
      Assert.fail("should throw exception");
    } catch (NoSuchMethodException e) {
      Assert.assertTrue("" + e, e.getMessage().contains("Unknown property 'nonExistingProperty'"));
    }

    // set value on read-only property
    try {
      testBean.getMapProperty().put("s", "s1Val");
      PropertyDescriptor pd = propertyUtilsBean.getPropertyDescriptor(testBean, "mapProperty");
      Class<?> type = propertyUtilsBean.getPropertyType(testBean, "mapProperty.s");

      propertyUtilsBean.setProperty(testBean, "mapProperty", Integer.valueOf(1));
      Assert.fail("should throw exception");
    } catch (Exception e) {
      Assert.assertTrue("" + e, e.getMessage().contains("Property 'mapProperty' has no setter method"));
    }

    // type mismatch
    try {
      propertyUtilsBean.setProperty(testBean, "intProp", "s1");
      Assert.fail("should throw exception");
    } catch (Exception e) {
      Assert.assertEquals(e.getClass(), IllegalArgumentException.class);
    }

    try {
      propertyUtilsBean.setProperty(testBean, "intProp", "1");
    } catch (IllegalArgumentException e) {
      // BeanUtils does not report invalid properties, but it handles type conversion, which above doesn't
      Assert.assertEquals("", 0, testBean.getIntProp());
      bub.setProperty(testBean, "intProp", "1"); // by default beanutils ignores conversion error
      Assert.assertEquals("", 1, testBean.getIntProp());
    }
  }

  public static JsonNode merge(JsonNode mainNode, JsonNode updateNode)
  {
    Iterator<String> fieldNames = updateNode.getFieldNames();
    while (fieldNames.hasNext()) {
      String fieldName = fieldNames.next();
      JsonNode jsonNode = mainNode.get(fieldName);
      // if field doesn't exist or is an embedded object
      if (jsonNode != null && jsonNode.isObject()) {
        merge(jsonNode, updateNode.get(fieldName));
      } else {
        if (mainNode instanceof ObjectNode) {
          // Overwrite field
          JsonNode value = updateNode.get(fieldName);
          ((ObjectNode)mainNode).put(fieldName, value);
        }
      }
    }
    return mainNode;
  }

}
