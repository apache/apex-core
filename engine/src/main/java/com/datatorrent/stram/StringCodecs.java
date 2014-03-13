/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.stram;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.Converter;

import com.datatorrent.api.StringCodec;
import com.datatorrent.common.util.DTThrowable;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class StringCodecs
{
  private static Map<Class<?>, Class<? extends StringCodec<?>>> codecs = new HashMap<Class<?>, Class<? extends StringCodec<?>>>();
  private static final Logger LOG = LoggerFactory.getLogger(StringCodecs.class);

  private StringCodecs()
  {
    // no creation of instances
  }

  static {
    LOG.debug("Loading default string converter");
    ConvertUtils.register(new Converter()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Object convert(Class type, Object value)
      {
        for (Class<?> clazz = value.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
          Class<? extends StringCodec> codec = codecs.get(clazz);
          if (codec == null) {
            continue;
          }

          StringCodec instance;
          try {
            instance = codec.newInstance();
          }
          catch (IllegalAccessException ex) {
            throw new RuntimeException("Internal Error - it's impossible for this exception to be thrown!", ex);
          }
          catch (InstantiationException ex) {
            throw new RuntimeException("Internal Error - it's impossible for this exception to be thrown!", ex);
          }

          return instance.toString(value);
        }

        return value.toString();
      }

    }, String.class);
  }

  public static void clear()
  {
    for (Map.Entry<Class<?>, Class<? extends StringCodec<?>>> entry : codecs.entrySet()) {
      ConvertUtils.deregister(entry.getKey());
    }
    codecs.clear();
  }

  public static void loadConverters(Map<Class<?>, Class<? extends StringCodec<?>>> map)
  {
    for (Map.Entry<Class<?>, Class<? extends StringCodec<?>>> entry : map.entrySet()) {
      try {
        register(entry.getValue(), entry.getKey());
      }
      catch (Exception ex) {
        DTThrowable.rethrow(ex);
      }
    }
  }

  public static <T> void deregister(Class<T> clazz)
  {
    ConvertUtils.deregister(clazz);
    codecs.remove(clazz);
  }

  public static <T> void register(final Class<? extends StringCodec<?>> codec, final Class<T> clazz) throws InstantiationException, IllegalAccessException
  {
    final StringCodec<?> codecInstance = codec.newInstance();
    ConvertUtils.register(new Converter()
    {
      @Override
      public Object convert(Class type, Object value)
      {
        return codecInstance.fromString(value.toString());
      }

    }, clazz);
    codecs.put(clazz, codec);
  }

}
