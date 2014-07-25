package com.datatorrent.api;

import java.io.Serializable;
import java.util.HashMap;

import com.datatorrent.common.util.DTThrowable;

import org.apache.commons.beanutils.BeanUtils;

/**
 * The attributes which represent arbitrary objects for which the schema cannot be
 * standardized, we allow them to be represented as <ClassName>:<String> representation.
 * This allows us to instantiate the class and setting the properties defined in the <String>.
 * <String> defines the properties in property=value format separated by :. If only the
 * <ClassName> is specified, then just the class is instantiated using default
 * constructor.
 *
 * @param <T> Type of the object which is converted to/from String
 *
 * Created by gaurav on 7/24/14.
 */
public class Bean2String<T> implements StringCodec<T>, Serializable
{

  public final String separator;
  public final String propertySeparator;

  public Bean2String()
  {
    separator = ":";
    propertySeparator = "=";
  }

  public Bean2String(String separator)
  {
    this.separator = separator;
    this.propertySeparator = "=";
  }

  public Bean2String(String separator, String propertySeparator)
  {
    this.separator = separator;
    this.propertySeparator = propertySeparator;
  }

  @Override
  public T fromString(String string)
  {
    if (string == null || string.isEmpty()) {
      return null;
    }
    String[] parts = string.split(separator, 2);
    try {
      @SuppressWarnings("unchecked")
      Class<? extends T> clazz = (Class<? extends T>) Thread.currentThread().getContextClassLoader().loadClass(parts[0]);
      if (parts.length == 1) {
        return clazz.newInstance();
      }
      T obj = clazz.newInstance();
      HashMap<String, String> hashMap = new HashMap<String, String>();
      String[] properties = parts[1].split(separator);
      for (String property : properties) {
        String[] keyValPair = property.split(propertySeparator, 2);
        hashMap.put(keyValPair[0], keyValPair[1]);
      }
      BeanUtils.populate(obj, hashMap);
      return obj;
    }
    catch (Throwable cause) {
      DTThrowable.rethrow(cause);
    }
    return null;
  }

  @Override
  public String toString(T pojo)
  {
    String arg = pojo.toString();
    if (arg == null) {
      return pojo.getClass().getCanonicalName();
    }
    return pojo.getClass().getCanonicalName() + separator + arg;
  }

}
