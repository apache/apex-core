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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.map.ObjectWriter;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;

import com.google.common.base.Throwables;

/**
 * This interface is essentially serializer/deserializer interface which works with String as
 * the serialized type. When initializing the attributes from the properties file, attribute
 * values represented as Strings are needed to be converted to POJO. This class facilitates the
 * conversion from and to String for attribute values.
 *
 * @param <T> Type of the object which can be converted to/from String.
 * @since 0.9.0
 */
public interface StringCodec<T>
{
  /**
   * Given a string representation (typically from properties file) for an object , create object from it.
   *
   * @param string Type of the POJO which is created from String representation.
   * @return POJO obtained as a result of deserialization
   */
  T fromString(String string);

  /**
   * Given a POJO, serialize it to a String object (typically to be stored in properties file).
   *
   * @param pojo The object which needs to be serialized.
   * @return Serialized representation of pojo..
   */
  String toString(T pojo);

  class Factory
  {
    public static StringCodec<?> getInstance(Class<?> cls)
    {
      if (cls == String.class) {
        return String2String.getInstance();
      } else if (cls == Integer.class) {
        return Integer2String.getInstance();
      } else if (cls == Long.class) {
        return Long2String.getInstance();
      } else if (cls == Boolean.class) {
        return Boolean2String.getInstance();
      } else if (Enum.class.isAssignableFrom(cls)) {
        return Enum2String.getInstance(cls);
      } else {
        return null;
      }
    }
  }

  class String2String implements StringCodec<String>, Serializable
  {
    @SuppressWarnings("deprecation")
    private static final String2String instance = new String2String();

    public static StringCodec<String> getInstance()
    {
      return instance;
    }

    /**
     * @deprecated As of release 3.5.0, replaced by {@link #getInstance()}
     */
    @Deprecated
    public String2String()
    {
    }

    @Override
    public String fromString(String string)
    {
      return string;
    }

    @Override
    public String toString(String pojo)
    {
      return pojo;
    }

    private static final long serialVersionUID = 201310141156L;
  }

  class Integer2String implements StringCodec<Integer>, Serializable
  {
    @SuppressWarnings("deprecation")
    private static final Integer2String instance = new Integer2String();

    public static StringCodec<Integer> getInstance()
    {
      return instance;
    }

    /**
     * @deprecated As of release 3.5.0, replaced by {@link #getInstance()}
     */
    @Deprecated
    public Integer2String()
    {
    }

    @Override
    public Integer fromString(String string)
    {
      return Integer.valueOf(string);
    }

    @Override
    public String toString(Integer pojo)
    {
      return String.valueOf(pojo);
    }

    private static final long serialVersionUID = 201310141157L;
  }

  class Long2String implements StringCodec<Long>, Serializable
  {
    @SuppressWarnings("deprecation")
    private static final Long2String instance = new Long2String();

    public static StringCodec<Long> getInstance()
    {
      return instance;
    }

    /**
     * @deprecated As of release 3.5.0, replaced by {@link #getInstance()}
     */
    @Deprecated
    public Long2String()
    {
    }

    @Override
    public Long fromString(String string)
    {
      return Long.valueOf(string);
    }

    @Override
    public String toString(Long pojo)
    {
      return String.valueOf(pojo);
    }

    private static final long serialVersionUID = 201310141158L;
  }

  class Boolean2String implements StringCodec<Boolean>, Serializable
  {
    @SuppressWarnings("deprecation")
    private static final Boolean2String instance = new Boolean2String();

    public static StringCodec<Boolean> getInstance()
    {
      return instance;
    }

    /**
     * @deprecated As of release 3.5.0, replaced by {@link #getInstance()}
     */
    @Deprecated
    public Boolean2String()
    {
    }

    @Override
    public Boolean fromString(String string)
    {
      return Boolean.valueOf(string);
    }

    @Override
    public String toString(Boolean pojo)
    {
      return String.valueOf(pojo);
    }

    private static final long serialVersionUID = 201310141159L;
  }

  /**
   * The attributes which represent arbitrary objects for which the schema cannot be
   * standardized, we allow them to be represented as <ClassName>:<Constructor_String>:<Property_String> representation.
   * This allows us to instantiate the class by invoking its constructor which takes
   * <String> as argument.  If only the <ClassName> is specified, then just the class is instantiated using default
   * constructor. If colon is specified then class is instantiated using constructor with
   * string as an argument.If properties are specified then properties will be set on the object. The properties
   * are defined in property=value format separated by colon(:)
   *
   * Note that the {@link #toString(Object) toString} method is by default NOT the proper reverse of the {@link
   * #fromString(String) fromString} method. In order for the {@link #toString(Object) toString} method to become a
   * proper reverse of the {@link #fromString(String) fromString} method, T's {@link T#toString() toString} method
   * must output null or <Constructor_String> or the <Constructor_String>:<Property_String> format as stated above.
   *
   * @param <T> Type of the object which is converted to/from String
   */
  class Object2String<T> implements StringCodec<T>, Serializable
  {
    @SuppressWarnings("deprecation")
    private static final Object2String instance = new Object2String();

    public static <T> StringCodec<T> getInstance()
    {
      return instance;
    }

    public static <T> StringCodec<T> getInstance(String separator)
    {
      return getInstance(separator, "=");
    }

    @SuppressWarnings("deprecation")
    public static <T> StringCodec<T> getInstance(String separator, String propertySeparator)
    {
      return new Object2String<>(separator, propertySeparator);
    }

    public final String separator;
    public final String propertySeparator;

    /**
     * @deprecated As of release 3.5.0, replaced by {@link #getInstance()}
     */
    @SuppressWarnings("deprecation")
    @Deprecated
    public Object2String()
    {
      this(":", "=");
    }

    /**
     * @deprecated As of release 3.5.0, replaced by {@link #getInstance(String)}
     */
    @SuppressWarnings("deprecation")
    @Deprecated
    public Object2String(String separator)
    {
      this(separator, "=");
    }

    /**
     * @deprecated As of release 3.5.0, replaced by {@link #getInstance(String, String)}
     */
    @Deprecated
    public Object2String(String separator, String propertySeparator)
    {
      this.separator = separator;
      this.propertySeparator = propertySeparator;
    }

    @Override
    @SuppressWarnings({"UseSpecificCatch", "BroadCatchBlock", "TooBroadCatch"})
    public T fromString(String string)
    {
      String[] parts = string.split(separator);

      try {
        @SuppressWarnings("unchecked")
        Class<? extends T> clazz = (Class<? extends T>)Thread.currentThread().getContextClassLoader().loadClass(parts[0]);
        if (parts.length == 1) {
          return clazz.newInstance();
        }

        //String[] properties = parts[1].split(separator, 2);
        if (parts.length == 2) {
          return clazz.getConstructor(String.class).newInstance(parts[1]);
        } else {
          T object = clazz.getConstructor(String.class).newInstance(parts[1]);
          HashMap<String, String> hashMap = new HashMap<>();
          for (int i = 2; i < parts.length; i++) {
            String[] keyValPair = parts[i].split(propertySeparator, 2);
            hashMap.put(keyValPair[0], keyValPair[1]);
          }
          BeanUtils.populate(object, hashMap);
          return object;
        }
      } catch (Throwable cause) {
        throw Throwables.propagate(cause);
      }
    }

    @Override
    public String toString(T pojo)
    {
      if (pojo == null) {
        return null;
      }
      String arg = pojo.toString();
      if (arg == null) {
        return pojo.getClass().getCanonicalName();
      }

      return pojo.getClass().getCanonicalName() + separator + arg;
    }

    private static final long serialVersionUID = 201311141853L;
  }

  class Map2String<K, V> implements StringCodec<Map<K, V>>, Serializable
  {
    @SuppressWarnings("deprecation")
    public static <K, V> StringCodec<Map<K, V>> getInstance(String separator, String equal, StringCodec<K> keyCodec, StringCodec<V> valueCodec)
    {
      return new Map2String<>(separator, equal, keyCodec, valueCodec);
    }

    private final StringCodec<K> keyCodec;
    private final StringCodec<V> valueCodec;
    private final String separator;
    private final String equal;

    /**
     * @deprecated As of release 3.5.0, replaced by {@link #getInstance(String, String, StringCodec, StringCodec)}
     */
    @Deprecated
    public Map2String(String separator, String equal, StringCodec<K> keyCodec, StringCodec<V> valueCodec)
    {
      this.equal = equal;
      this.separator = separator;
      this.keyCodec = keyCodec;
      this.valueCodec = valueCodec;
    }

    @Override
    public Map<K, V> fromString(String string)
    {
      if (string == null) {
        return null;
      }

      if (string.isEmpty()) {
        return new HashMap<>();
      }

      String[] parts = string.split(separator);
      HashMap<K, V> map = new HashMap<>();
      for (String part : parts) {
        String[] kvpair = part.split(equal, 2);
        map.put(keyCodec.fromString(kvpair[0]), valueCodec.fromString(kvpair[1]));
      }

      return map;
    }

    @Override
    public String toString(Map<K, V> map)
    {
      if (map == null) {
        return null;
      }

      if (map.isEmpty()) {
        return "";
      }
      String[] parts = new String[map.size()];
      int i = 0;
      for (Map.Entry<K, V> entry : map.entrySet()) {
        parts[i++] = keyCodec.toString(entry.getKey()) + equal + valueCodec.toString(entry.getValue());
      }
      return StringUtils.join(parts, separator);
    }

    private static final long serialVersionUID = 201402272053L;
  }

  class Collection2String<T> implements StringCodec<Collection<T>>, Serializable
  {
    @SuppressWarnings("deprecation")
    public static <T> StringCodec<Collection<T>> getInstance(String separator, StringCodec<T> codec)
    {
      return new Collection2String<>(separator, codec);
    }

    private final String separator;
    private final StringCodec<T> codec;

    /**
     * @deprecated As of release 3.5.0, replaced by {@link #getInstance(String, StringCodec)}
     */
    @Deprecated
    public Collection2String(String separator, StringCodec<T> codec)
    {
      this.separator = separator;
      this.codec = codec;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<T> fromString(String string)
    {
      if (string == null) {
        return null;
      }

      if (string.isEmpty()) {
        return Collections.EMPTY_LIST;
      }

      String[] parts = string.split(separator);
      ArrayList<T> arrayList = new ArrayList<>(parts.length);
      for (String part : parts) {
        arrayList.add(codec.fromString(part));
      }

      return arrayList;
    }

    @Override
    public String toString(Collection<T> pojo)
    {
      if (pojo == null) {
        return null;
      }

      if (pojo.isEmpty()) {
        return "";
      }

      String[] parts = new String[pojo.size()];

      int i = 0;
      for (T o : pojo) {
        parts[i++] = codec.toString(o);
      }

      return StringUtils.join(parts, separator);
    }

    private static final long serialVersionUID = 201401091806L;
  }

  class Enum2String<T extends Enum<T>> implements StringCodec<T>, Serializable
  {
    private final Class<T> clazz;

    @SuppressWarnings("deprecation")
    public static Enum2String getInstance(Class clazz)
    {
      return new Enum2String(clazz);
    }

    /**
     * @deprecated As of release 3.5.0, replaced by {@link #getInstance(Class<T>)}
     */
    @Deprecated
    public Enum2String(Class<T> clazz)
    {
      this.clazz = clazz;
    }

    @Override
    public T fromString(String string)
    {
      return Enum.valueOf(clazz, string);
    }

    @Override
    public String toString(T pojo)
    {
      return pojo.name();
    }

    private static final long serialVersionUID = 201310181757L;
  }

  class Class2String<T> implements StringCodec<Class<? extends T>>, Serializable
  {
    @SuppressWarnings("deprecation")
    private static final StringCodec instance = new Class2String<>();

    public static <T> StringCodec<Class<? extends T>> getInstance()
    {
      return (StringCodec<Class<? extends T>>)instance;
    }

    /**
     * @deprecated As of release 3.5.0, replaced by {@link #getInstance()}
     */
    public Class2String()
    {
    }

    @Override
    @SuppressWarnings({"BroadCatchBlock", "TooBroadCatch"})
    public Class<? extends T> fromString(String string)
    {
      try {
        @SuppressWarnings({"rawtypes", "unchecked"})
        Class<? extends T> clazz = (Class)Thread.currentThread().getContextClassLoader().loadClass(string);
        return clazz;
      } catch (Throwable cause) {
        throw Throwables.propagate(cause);
      }
    }

    @Override
    public String toString(Class<? extends T> clazz)
    {
      return clazz.getCanonicalName();
    }

    private static final long serialVersionUID = 201312082053L;
  }

  class JsonStringCodec<T> implements StringCodec<T>, Serializable
  {
    private static final long serialVersionUID = 2513932518264776006L;

    @SuppressWarnings("deprecation")
    public static <T> StringCodec<T> getInstance(Class<T> clazz)
    {
      return new JsonStringCodec<>(clazz);
    }

    Class<?> clazz;

    /**
     * @deprecated As of release 3.5.0, replaced by {@link #getInstance(Class)}
     */
    public JsonStringCodec(Class<T> clazz)
    {
      this.clazz = clazz;
    }

    @Override
    public T fromString(String string)
    {
      try {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        ObjectReader reader = mapper.reader(clazz);
        return reader.readValue(string);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public String toString(T pojo)
    {
      try {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        ObjectWriter writer = mapper.writer();
        return writer.writeValueAsString(pojo);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
