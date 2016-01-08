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
package com.datatorrent.common.util;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper base class to ensure that Java Serializer is able to work with the transient final
 * fields in the class which extends this class. By definition the transient fields are not serialized.
 * But when such a field is declared final in the class, there is no way to initialize it either. So
 * this class provides boilerplate code to play with the very uncommon serialization API and provides a
 * way to easily and correctly serialize and deserialize such objects. The only condition is that the
 * class of such object should provide either a copy constructor or a default constructor. *
 *
 * @since 3.0.0
 */
public class SerializableObject implements Serializable
{
  /**
   * Create an instance of this class by invoking the copy constructor.
   * If the copy constructor does not exist, this method tries to get
   * a new instance using default constructor and then initializing all
   * the non final or non transient fields of the new instance with the
   * corresponding values from the deserialized operator. Users can override
   * this method to invoke non default constructor. They can override
   * transferState to just copy over the non final or non transient values
   * from various fields of the src object.
   *
   * @return an instance of the operator.
   * @throws ObjectStreamException
   */
  public Object readResolve() throws ObjectStreamException
  {
    try {
      Constructor<? extends SerializableObject> constructor = this.getClass().getConstructor(this.getClass());
      try {
        constructor.setAccessible(true);
      } catch (SecurityException ex) {
        logger.warn("Accessing copy constructor {} failed.", constructor, ex);
      }
      try {
        return constructor.newInstance(this);
      } catch (ReflectiveOperationException | IllegalArgumentException ex) {
        throw new RuntimeException("Instantiation using copy constructor failed!", ex);
      }
    } catch (NoSuchMethodException snme) {
      logger.debug("No copy constructor detected for class {}, trying default constructor.", this.getClass().getSimpleName());
      try {
        SerializableObject newInstance = this.getClass().newInstance();
        transferStateTo(newInstance);
        return newInstance;
      } catch (ReflectiveOperationException ex) {
        throw new RuntimeException("Deserialization using default constructor failed!", ex);
      }
    }
  }

  /**
   * Copy over non final and non transient values from src to dest object.
   *
   * @param dest The object to which values are copied.
   */
  public void transferStateTo(Object dest)
  {
    for (Class<?> clazz = getClass(); !clazz.equals(Object.class); clazz = clazz.getSuperclass()) {
      Field[] fields = clazz.getDeclaredFields();
      for (Field field: fields) {
        final int modifiers = field.getModifiers();
        if (!(Modifier.isFinal(modifiers) && Modifier.isTransient(modifiers) || Modifier.isStatic(modifiers))) {
          try {
            field.setAccessible(true);
          } catch (SecurityException ex) {
            logger.warn("Cannot set field {} accessible.", field, ex);
          }
          try {
            field.set(dest, field.get(this));
          } catch (IllegalArgumentException ex) {
            throw new RuntimeException("Getter/Setter argument failed using reflection on " + field, ex);
          } catch (IllegalAccessException ex) {
            throw new RuntimeException("Getter/Setter access failed using reflection on " + field, ex);
          }
          if (!field.getType().isPrimitive()) {
            try {
              field.set(this, null);
            } catch (IllegalArgumentException | IllegalAccessException ex) {
              logger.warn("Failed to set field {} to null; generally it's harmless, but with reference counted data structure this may be an issue.", field, ex);
            }
          }
        }
      }
    }
  }

  private static final long serialVersionUID = 201505211622L;
  private static final Logger logger = LoggerFactory.getLogger(SerializableObject.class);
}
