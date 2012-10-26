/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api;

import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.AttributeMap;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * The base interface for context for all of the streaming platform objects<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface Context extends io.netty.util.AttributeMap
{
  public static final AttributeKey<Integer> INPUT_PORT_BUFFER_SIZE = Keys.add(Integer.class, "INPUT_PORT_BUFFER_SIZE");
  public static final AttributeKey<Integer> OPERATOR_SPIN_MILLIS = Keys.add(Integer.class, "OPERATOR_SPIN_MILLIS");

  /**
   * The set of keys is closed to allow for enumeration and serialization of attribute maps.
   * Parameterized enum for the keys would have been welcome here!
   */
  static final class Keys {
    private static final Map<String, AttributeKey<?>> KEYS = new HashMap<String, AttributeKey<?>>();

    // ideally we would also extend AttributeKey, but it was made final..
    private static <T> AttributeKey<T> add(Class<T> type, String name) {
      AttributeKey<T> key = new AttributeKey<T>(name);
      KEYS.put(name, key);
      return key;
    }

  }

  /**
   * Attribute map records values against String keys and can be serialized
   * ({@link AttributeKey} cannot be serialized)
   */
  public class SerializableAttributeMap implements AttributeMap, Serializable {
    private static final long serialVersionUID = 1L;
    private final Map<String, DefaultAttribute<?>> map = new HashMap<String, DefaultAttribute<?>>();

    @Override
    public <T> Attribute<T> attr(AttributeKey<T> key) {
      @SuppressWarnings("unchecked")
      DefaultAttribute<T> attr = (DefaultAttribute<T>) map.get(key.name());
      if (attr == null) {
          attr = new DefaultAttribute<T>();
          map.put(key.name(), attr);
      }
      return attr;
    }

    private class DefaultAttribute<T> extends AtomicReference<T> implements Attribute<T>, Serializable {
      private static final long serialVersionUID = -2661411462200283011L;

      @Override
      public T setIfAbsent(T value) {
          if (compareAndSet(null, value)) {
              return null;
          } else {
              return get();
          }
      }

      @Override
      public void remove() {
          set(null);
      }
    }

    /**
     * Set values in the target map.
     * @param target
     */
    @SuppressWarnings("unchecked")
    public void copyValues(AttributeMap target) {
      for (Map.Entry<String, DefaultAttribute<?>> e : map.entrySet()) {
        @SuppressWarnings("rawtypes")
        AttributeKey key = Keys.KEYS.get(e.getKey());
        if (key == null) {
          throw new IllegalStateException("Unknown key: " + e.getKey());
        }
        target.attr(key).set(e.getValue().get());
      }
    }
  }

}
