/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.util;

import io.netty.util.Attribute;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Parameterized and scoped context attribute map that supports serialization.
 * Derived from {@link io.netty.util.AttributeMap}
 */
public interface AttributeMap<CONTEXT> {

  <T> Attribute<T> attr(AttributeKey<CONTEXT, T> key);

  /**
   * Scoped attribute key. Subclasses define scope.
   */
  abstract public static class AttributeKey<CONTEXT, T> {
    private static final ConcurrentMap<String, AttributeKey<?, ?>> keys = new ConcurrentHashMap<String, AttributeKey<?, ?>>();

    private final Class<CONTEXT> scope;
    private final String name;

    protected AttributeKey(Class<CONTEXT> scope, String name) {
      this.scope = scope;
      this.name = name;
      keys.put(stringKey(scope, name), this);
    }

    public String name() {
      return name;
    }

    private static String stringKey(Class<?> scope, String name) {
      return scope.getName() + "." + name;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static <CONTEXT, T> AttributeKey<CONTEXT, T> getKey(Class<CONTEXT> scope, String key) {
      return (AttributeKey)keys.get(stringKey(scope, key));
    }
  }

  /**
   * Attribute map records values against String keys and can therfore be serialized
   * ({@link AttributeKey} cannot be serialized)
   */
  public class DefaultAttributeMap<CONTEXT> implements AttributeMap<CONTEXT>, Serializable {
    private static final long serialVersionUID = 1L;
    private final Map<String, DefaultAttribute<?>> map = new HashMap<String, DefaultAttribute<?>>();
    // serialize scope if there is at least one attribute for lookup on reload
    private Class<CONTEXT> scope;

    @Override
    public <T> Attribute<T> attr(AttributeKey<CONTEXT, T> key) {
      @SuppressWarnings("unchecked")
      DefaultAttribute<T> attr = (DefaultAttribute<T>) map.get(key.name());
      if (attr == null) {
        if (scope == null) {
          scope = key.scope;
        } else {
          if (scope != key.scope) {
            throw new IllegalArgumentException("Invalid scope: " + scope.getName());
          }
        }
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
     * Set all values in target map.
     * @param target
     */
    public void copyTo(AttributeMap<CONTEXT> target) {
      for (Map.Entry<String, DefaultAttribute<?>> e : map.entrySet()) {
        AttributeKey<CONTEXT, Object> key = AttributeKey.getKey(this.scope, e.getKey());
        if (key == null) {
          throw new IllegalStateException("Unknown key: " + e.getKey());
        }
        target.attr(key).set(e.getValue().get());
      }
    }
  }

}
