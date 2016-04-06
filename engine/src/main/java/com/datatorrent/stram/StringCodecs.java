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
package com.datatorrent.stram;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.Converter;

import com.google.common.base.Throwables;

import com.datatorrent.api.StringCodec;

/**
 * <p>StringCodecs class.</p>
 *
 * @since 0.9.4
 */
public class StringCodecs
{
  private static final ConcurrentHashMap<ClassLoader, Boolean> classLoaders = new ConcurrentHashMap<>();
  private static final Map<Class<?>, Class<? extends StringCodec<?>>> codecs = new HashMap<>();
  private static final Logger LOG = LoggerFactory.getLogger(StringCodecs.class);

  private StringCodecs()
  {
    // no creation of instances
  }

  public static void loadDefaultConverters()
  {
    LOG.debug("Loading default converters for BeanUtils");
    ConvertUtils.register(new Converter()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Object convert(Class type, Object value)
      {
        if (value == null) {
          return null;
        }
        for (Class<?> clazz = value.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
          Class<? extends StringCodec> codec = codecs.get(clazz);
          if (codec == null) {
            continue;
          }

          StringCodec instance;
          try {
            instance = codec.newInstance();
          } catch (IllegalAccessException ex) {
            throw new RuntimeException("Internal Error - it's impossible for this exception to be thrown!", ex);
          } catch (InstantiationException ex) {
            throw new RuntimeException("Internal Error - it's impossible for this exception to be thrown!", ex);
          }

          return instance.toString(value);
        }

        return value.toString();
      }

    }, String.class);

    ConvertUtils.register(new Converter()
    {
      @Override
      public Object convert(Class type, Object value)
      {
        return value == null ? null : URI.create(value.toString());
      }
    }, URI.class);
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
    check();
    if (map == null) {
      return;
    }
    for (Map.Entry<Class<?>, Class<? extends StringCodec<?>>> entry : map.entrySet()) {
      try {
        register(entry.getValue(), entry.getKey());
      } catch (Exception ex) {
        throw Throwables.propagate(ex);
      }
    }
  }

  public static void check()
  {
    if (classLoaders.putIfAbsent(Thread.currentThread().getContextClassLoader(), Boolean.TRUE) == null) {
      loadDefaultConverters();
      for (Map.Entry<Class<?>, Class<? extends StringCodec<?>>> entry : codecs.entrySet()) {
        try {
          final StringCodec<?> codecInstance = entry.getValue().newInstance();
          ConvertUtils.register(new Converter()
          {
            @Override
            public Object convert(Class type, Object value)
            {
              return value == null ? null : codecInstance.fromString(value.toString());
            }

          }, entry.getKey());
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
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
    check();
    final StringCodec<?> codecInstance = codec.newInstance();
    ConvertUtils.register(new Converter()
    {
      @Override
      public Object convert(Class type, Object value)
      {
        return value == null ? null : codecInstance.fromString(value.toString());
      }

    }, clazz);
    codecs.put(clazz, codec);
  }

}
