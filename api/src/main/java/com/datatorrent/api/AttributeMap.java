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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Parameterized and scoped context attribute map that supports serialization.
 * Derived from io.netty.util.AttributeMap
 *
 * @since 0.3.2
 */
public interface AttributeMap
{
  public interface Attribute<T>
  {
    T get();

    void set(T value);

    T getAndSet(T value);

    T setIfAbsent(T value);

    boolean compareAndSet(T oldValue, T newValue);

    void remove();

  }

  /**
   * Return the attribute value for the given key. If the map does not have an
   * entry for the key, a default attribute value will be returned.
   *
   * @param <T>
   * @param key
   * @return <T> Attribute<T>
   */
  <T> Attribute<T> attr(AttributeKey<T> key);

  /**
   * Return the value map
   *
   * @return the value map
   */
  Map<String, Object> valueMap();

  /**
   * Add non-durable attribute.
   *
   * This attribute will not get serialized across processes. As the process dies, these attributes disappear.
   * @param <T>
   * @param key
   * @return
   */
  <T> Attribute<T> addTransientAttribute(AttributeKey<T> key);

  /**
   * Scoped attribute key. Subclasses define scope.
   *
   * @param <T>
   */
  public static class AttributeKey<T>
  {
    private static final ConcurrentMap<String, AttributeKey<?>> keys = new ConcurrentHashMap<String, AttributeKey<?>>();
    private final Class<?> scope;
    private final String name;

    @SuppressWarnings("LeakingThisInConstructor")
    public AttributeKey(Class<?> scope, String name)
    {
      this.scope = scope;
      this.name = name;
      keys.put(stringKey(scope, name), this);
    }

    public String name()
    {
      return name;
    }

    private static String stringKey(Class<?> scope, String name)
    {
      return scope.getName() + "." + name;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T> AttributeKey<T> getKey(Class<?> scope, String key)
    {
      return (AttributeKey)keys.get(stringKey(scope, key));
    }

  }

  /**
   * Attribute map records values against String keys and can therefore be serialized
   * ({@link AttributeKey} cannot be serialized)
   *
   */
  public class DefaultAttributeMap implements AttributeMap, Serializable
  {
    private static final long serialVersionUID = 201306051022L;
    private final Map<String, DefaultAttribute<?>> map = new HashMap<String, DefaultAttribute<?>>();
    // if there is at least one attribute, serialize scope for key object lookup
    private final Class<?> scope;
    public DefaultAttributeMap(Class<?> scope)
    {
      this.scope = scope;
    }

    @Override
    public <T> Attribute<T> attr(AttributeKey<T> key)
    {
      @SuppressWarnings("unchecked")
      DefaultAttribute<T> attr = (DefaultAttribute<T>)map.get(key.name());
      if (attr == null && scope == key.scope) {
        attr = new DefaultAttribute<T>();
        map.put(key.name(), attr);
      }
      return attr;
    }

    @Override
    public Map<String, Object> valueMap()
    {
      Map<String, Object> valueMap = new HashMap<String, Object>();
      for (Map.Entry<String, DefaultAttribute<?>> entry : this.map.entrySet()) {
        valueMap.put(entry.getKey(), entry.getValue().get());
      }
      return valueMap;
    }

    @Override
    public <T> Attribute<T> addTransientAttribute(AttributeKey<T> key)
    {
      @SuppressWarnings("unchecked")
      DefaultAttribute<T> attr = (DefaultAttribute<T>)map.get(key.name());
      if (attr == null) {
        attr = new DefaultAttribute<T>();
        map.put(key.name(), attr);
      }

      return attr;
    }

    private class DefaultAttribute<T> extends AtomicReference<T> implements Attribute<T>, Serializable
    {
      private static final long serialVersionUID = -2661411462200283011L;

      @Override
      public T setIfAbsent(T value)
      {
        if (compareAndSet(null, value)) {
          return null;
        }
        else {
          return get();
        }
      }

      @Override
      public void remove()
      {
        set(null);
      }

    }

    /**
     * Set all values in target map.
     *
     * @param target
     */
    public void copyTo(AttributeMap target)
    {
      for (Map.Entry<String, DefaultAttribute<?>> e : map.entrySet()) {
        AttributeKey<Object> key = AttributeKey.getKey(this.scope, e.getKey());
        if (key == null) {
          throw new IllegalStateException("Unknown key: " + e.getKey());
        }
        target.attr(key).set(e.getValue().get());
      }
    }

    @Override
    public String toString()
    {
      return this.map.toString();
    }

  }

}
