package com.datatorrent.api;

import java.io.Serializable;
import java.util.HashMap;

import com.datatorrent.common.util.DTThrowable;

import org.apache.commons.beanutils.BeanUtils;

/**
 * Created by gaurav on 7/24/14.
 */
public class Bean2String<T> implements StringCodec<T>, Serializable
{

  public final String separator;
  public final String equal;

  public Bean2String()
  {
    separator = ":";
    equal = "=";
  }

  public Bean2String(String separator)
  {
    this.separator = separator;
    this.equal = "=";
  }

  public Bean2String(String separator, String equal)
  {
    this.separator = separator;
    this.equal = equal;
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
        String[] kvpair = property.split(equal, 2);
        hashMap.put(kvpair[0], kvpair[1]);
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
