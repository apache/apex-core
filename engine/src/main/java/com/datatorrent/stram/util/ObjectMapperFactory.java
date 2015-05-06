/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.util;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.annotate.JsonMethod;
import org.codehaus.jackson.annotate.JsonTypeInfo.As;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.introspect.AnnotatedField;
import org.codehaus.jackson.map.introspect.AnnotatedMember;
import org.codehaus.jackson.map.introspect.AnnotatedMethod;
import org.codehaus.jackson.map.introspect.VisibilityChecker;

/**
 * <p>ObjectMapperFactory</p>
 * A centred place to manage the configuration of jackson ObjectMapper for serialize/deserialize operators
 *
 * @since 2.1
 */
public class ObjectMapperFactory
{

  public static ObjectMapper getOperatorValueSerializer()
  {
    ObjectMapper returnVal = new ObjectMapper();
    returnVal.setVisibilityChecker(new VC());
    returnVal.configure(org.codehaus.jackson.map.SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
    returnVal.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL, As.WRAPPER_OBJECT);
    return returnVal;
  }

  public static ObjectMapper getOperatorValueDeserializer()
  {    
    ObjectMapper returnVal = new ObjectMapper();
    returnVal.configure(org.codehaus.jackson.map.DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    returnVal.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL, As.WRAPPER_OBJECT);
    return returnVal;
  }
  
  
  /**
   * This class filter out all direct field access and fields with one of setter/getter
   */
  public static class VC implements VisibilityChecker<VC>{

    @Override
    public VC with(JsonAutoDetect ann)
    {
      return this;
    }

    @Override
    public VC with(Visibility v)
    {
      return this;
    }

    @Override
    public VC withVisibility(JsonMethod method, Visibility v)
    {
      return this;
    }

    @Override
    public VC withGetterVisibility(Visibility v)
    {
      return this;
    }

    @Override
    public VC withIsGetterVisibility(Visibility v)
    {
      return this;
    }

    @Override
    public VC withSetterVisibility(Visibility v)
    {
      return this;
    }

    @Override
    public VC withCreatorVisibility(Visibility v)
    {
      return this;
    }

    @Override
    public VC withFieldVisibility(Visibility v)
    {
      return this;
    }

    @Override
    public boolean isGetterVisible(Method m)
    {
      if(m == null || !Modifier.isPublic(m.getModifiers())){
        return false;
      }
      try {
        PropertyDescriptor[] pds = Introspector.getBeanInfo(m.getDeclaringClass()).getPropertyDescriptors();
        for (PropertyDescriptor pd : pds) {
          if(pd.getReadMethod() != null && pd.getReadMethod().equals(m)){
            Method setter = pd.getWriteMethod();
            if(setter == null || !Modifier.isPublic(setter.getModifiers())){
              return false;
            } else {
              return true;
            }
          }
        }
      } catch (IntrospectionException e) {
        return false;
      }
      return false;
    }

    @Override
    public boolean isGetterVisible(AnnotatedMethod m)
    {
      return isGetterVisible(m.getAnnotated());
    }

    @Override
    public boolean isIsGetterVisible(Method m)
    {
      return isGetterVisible(m);
    }

    @Override
    public boolean isIsGetterVisible(AnnotatedMethod m)
    {
      return isIsGetterVisible(m.getAnnotated());
    }

    @Override
    public boolean isSetterVisible(Method m)
    {
      if(m == null || !Modifier.isPublic(m.getModifiers())){
        return false;
      }
      try {
        PropertyDescriptor[] pds = Introspector.getBeanInfo(m.getDeclaringClass()).getPropertyDescriptors();
        for (PropertyDescriptor pd : pds) {
          if(pd.getWriteMethod() != null && pd.getWriteMethod().equals(m)){
            Method getter = pd.getReadMethod();
            if(getter == null || !Modifier.isPublic(getter.getModifiers())){
              return false;
            } else {
              return true;
            }
          }
        }
      } catch (IntrospectionException e) {
        return false;
      }
      return false;
    }

    @Override
    public boolean isSetterVisible(AnnotatedMethod m)
    {
      return isSetterVisible(m.getAnnotated());
    }

    @Override
    public boolean isCreatorVisible(Member m)
    {
      return false;
    }

    @Override
    public boolean isCreatorVisible(AnnotatedMember m)
    {
      return false;
    }

    @Override
    public boolean isFieldVisible(Field f)
    {
      return false;
    }

    @Override
    public boolean isFieldVisible(AnnotatedField f)
    {
      return false;
    }
    
  }

}
