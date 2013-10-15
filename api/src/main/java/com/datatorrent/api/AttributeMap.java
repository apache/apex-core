/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.api;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.StringCodec.Boolean2String;
import com.datatorrent.api.StringCodec.Integer2String;
import com.datatorrent.api.StringCodec.Long2String;
import com.datatorrent.api.StringCodec.String2String;

/**
 * Parameterized and scoped context attribute map that supports serialization.
 * Derived from io.netty.util.AttributeMap
 *
 * @since 0.3.2
 */
public interface AttributeMap
{
  /**
   * Return the attribute value for the given key. If the map does not have an
   * entry for the key, a default attribute value will be created and returned.
   * Modifies state of the map of key is not present.
   *
   * @param <T>
   * @param key
   * @return <T> AttributeValue<T>
   */
  <T> T get(Attribute<T> key);

  <T> T get(String key);

  <T> T put(Attribute<T> key, T value);

  Set<Map.Entry<Attribute<?>, Object>> entrySet();

  /**
   * Return the value map
   *
   * @return the value map
   */
  AttributeMap clone();

  /**
   * Scoped attribute key. Subclasses define scope.
   *
   * @param <T>
   */
  public static class Attribute<T> implements Serializable
  {
    public final T defaultValue;
    public final String name;
    public final StringCodec<T> codec;

    public Attribute(StringCodec<T> codec)
    {
      this(null, null, codec);
    }

    public Attribute(T defaultValue)
    {
      this(null, defaultValue, null);
    }

    public Attribute(T defaultValue, StringCodec<T> codec)
    {
      this(null, defaultValue, codec);
    }

    private Attribute(String name, T defaultValue, StringCodec<T> codec)
    {
      this.name = name;
      this.defaultValue = defaultValue;
      this.codec = codec;
    }

    @Override
    public int hashCode()
    {
      return name.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      @SuppressWarnings("unchecked")
      final Attribute<T> other = (Attribute<T>)obj;
      if ((this.name == null) ? (other.name != null) : !this.name.equals(other.name)) {
        return false;
      }
      return true;
    }

    @Override
    public String toString()
    {
      return "Attribute{" + "defaultValue=" + defaultValue + ", name=" + name + ", clazz=" + codec + '}';
    }

    private static final long serialVersionUID = 201310111904L;
  }

  /**
   * AttributeValue map records values against String keys and can therefore be serialized
   * ({@link Attribute} cannot be serialized)
   *
   */
  public class DefaultAttributeMap implements AttributeMap, Serializable
  {
    private static final long serialVersionUID = 201306051022L;
    private final HashMap<Attribute<?>, Object> map;
    private final HashMap<String, Attribute<?>> attributeMap;

    public DefaultAttributeMap()
    {
      this(new HashMap<Attribute<?>, Object>());
    }

    private DefaultAttributeMap(HashMap<Attribute<?>, Object> map)
    {
      this.map = map;
      attributeMap = new HashMap<String, Attribute<?>>(map.size());
      for (Attribute<?> attribute : map.keySet()) {
        attributeMap.put(attribute.name, attribute);
      }
    }

    // if there is at least one attribute, serialize scope for key object lookup
    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(Attribute<T> key)
    {
      return (T)map.get(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(String key)
    {
      Attribute<?> attribute = attributeMap.get(key);
      if (attribute != null) {
        return (T)get(attribute);
      }

      return null;
    }

    @Override
    public String toString()
    {
      return this.map.toString();
    }

    @Override
    @SuppressWarnings("unchecked")
    public DefaultAttributeMap clone()
    {
      return new DefaultAttributeMap((HashMap<Attribute<?>, Object>)map.clone());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T put(Attribute<T> key, T value)
    {
      attributeMap.put(key.name, key);
      return (T)map.put(key, value);
    }

    @Override
    public Set<Entry<Attribute<?>, Object>> entrySet()
    {
      return map.entrySet();
    }

  }

  /**
   * This class inspects and initializes the attributes with their field names so that they can be used
   * from properties files. The engine initializes this class and
   */
  public static class AttributeInitializer
  {
    private static final Logger logger = LoggerFactory.getLogger(AttributeInitializer.class);
    static final HashMap<Class<?>, Set<AttributeMap.Attribute<Object>>> map = new HashMap<Class<?>, Set<AttributeMap.Attribute<Object>>>();

    public static Set<AttributeMap.Attribute<Object>> getAttributes(Class<?> clazz)
    {
      return map.get(clazz);
    }

    public static long initialize(final Class<?> clazz)
    {
      if (map.containsKey(clazz)) {
        return 0;
      }

      Set<AttributeMap.Attribute<Object>> set = new HashSet<AttributeMap.Attribute<Object>>();
      try {
        for (Field f: clazz.getDeclaredFields()) {
          if (Modifier.isStatic(f.getModifiers()) && AttributeMap.Attribute.class.isAssignableFrom(f.getType())) {
            @SuppressWarnings(value = "unchecked")
            AttributeMap.Attribute<Object> attribute = (AttributeMap.Attribute<Object>)f.get(null);

            if (attribute.name == null) {
              Field nameField = AttributeMap.Attribute.class.getDeclaredField("name");
              nameField.setAccessible(true);
              nameField.set(attribute, clazz.getCanonicalName() + '.' + f.getName());
              nameField.setAccessible(false);
            }
            /* Handle trivial cases here even though this may spoil API users. */
            if (attribute.codec == null) {
              StringCodec<?> codec = null;
              if (attribute.defaultValue != null) {
                Class<?> klass = attribute.defaultValue.getClass();
                if (klass == String.class) {
                  codec = new String2String();
                }
                else if (klass == Integer.class) {
                  codec = new Integer2String();
                }
                else if (klass == Long.class) {
                  codec = new Long2String();
                }
                else if (klass == Boolean.class) {
                  codec = new Boolean2String();
                }
              }
              if (codec == null) {
                logger.warn("Attribute {}.{} cannot be specified in the properties file as it does not have a StringCodec defined!", clazz.getSimpleName(), f.getName());
              }
              else {
                Field codecField = AttributeMap.Attribute.class.getDeclaredField("codec");
                codecField.setAccessible(true);
                codecField.set(attribute, codec);
                codecField.setAccessible(false);
              }
            }
            set.add(attribute);
          }
        }
      }
      catch (Exception ex) {
        throw new RuntimeException(ex);
      }
      map.put(clazz, set);
      return System.identityHashCode(clazz);
    }
  }

}
