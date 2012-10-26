/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.moduleexperiment;

import java.lang.reflect.Field;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import org.apache.commons.beanutils.ConvertUtilsBean;
import org.apache.commons.beanutils.Converter;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.MembersInjector;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;
import com.malhartech.annotation.InjectConfig;

public class ValidationTest {

  private static Logger LOG = LoggerFactory.getLogger(ValidationTest.class);

  public class MyBean{
    @NotNull
    @Pattern(regexp=".*malhar.*", message="Value has to contain 'malhar'!")
    String x;

    @Min(2)
    int y;

    @InjectConfig(key="stringKey")
    private String stringField;

    @InjectConfig(key="urlKey")
    private java.net.URL urlField;

    @InjectConfig(key="stringArrayKey")
    private String[] stringArrayField;

  }


  public class TestGuiceModule extends AbstractModule {
    final Configuration conf;

    public TestGuiceModule(Configuration conf) {
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
    private class ConfigurableListener implements TypeListener {
      @Override
      public <T> void hear(TypeLiteral<T> typeLiteral, TypeEncounter<T> typeEncounter) {
        for (Class<?> c = typeLiteral.getRawType(); c != Object.class; c = c.getSuperclass())
        {
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
    private class ConfigurationInjector<T> implements MembersInjector<T> {
      private final Field field;
      private final InjectConfig annotation;

      ConfigurationInjector(Field field, InjectConfig annotation) {
        this.field = field;
        this.annotation = annotation;
        field.setAccessible(true);
      }

      @Override
      public void injectMembers(T t) {
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
    protected void configure() {
      bindListener(Matchers.any(), new ConfigurableListener());
      bind(Configuration.class).toInstance(conf);
    }
  }


  public class MyBeanExt extends MyBean {
    @InjectConfig(key="anotherStringKey")
    private String anotherInjectableField;
  }


  @Test
  public void testBinding() throws Exception {

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
    Assert.assertArrayEquals("", new String[] {"a", "b", "c"}, bean.stringArrayField);
  }

}
