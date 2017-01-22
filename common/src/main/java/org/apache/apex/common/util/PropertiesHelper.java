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
package org.apache.apex.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

@Evolving
public class PropertiesHelper
{
  /**
   * Reading system property as long value.
   * @param propertyName Name of the system property
   * @param defaultValue Default value to return in case of an error, out of range etc.
   * @param minValue minimum valid value
   * @param maxValue maximum valid value
   * @return returns the value if it is between min and max value(inclusive), otherwise default value is returned.
   */
  public static long getLong(final String propertyName, final long defaultValue, final long minValue, final long maxValue)
  {

    PropertyReader<Long> propertyReader = new PropertyReader<Long>()
    {
      @Override
      protected Long valueOf(String property)
      {
        long value = Long.decode(property);
        if (value < minValue || value > maxValue) {
          logger.warn("Property {} is outside the range [{},{}], setting to default {}", propertyName, minValue, maxValue, defaultValue);
          return null;
        }

        return value;
      }
    };

    return propertyReader.getValue(propertyName, defaultValue);
  }

  /**
   * Reading system property as long value.
   * @param propertyName Name of the system property
   * @param defaultValue Default value to return in case of an error
   * @return returns the value if it is valid or return invalid.
   */
  public static boolean getBoolean(String propertyName, boolean defaultValue)
  {
    PropertyReader<Boolean> propertyReader = new PropertyReader<Boolean>()
    {
      @Override
      protected Boolean valueOf(String property)
      {
        return Boolean.valueOf(property);
      }
    };

    return propertyReader.getValue(propertyName, defaultValue);
  }

  public abstract static class PropertyReader<T>
  {
    public T getValue(String propertyName, T defaultValue)
    {
      String property = System.getProperty(propertyName);
      T result = defaultValue;
      if (property != null) {
        try {
          T value = valueOf(property);
          if (value == null) {
            logger.warn("Property {} is invalid, setting to default {}", propertyName, defaultValue);
          } else {
            result = value;
          }
        } catch (Exception ex) {
          logger.warn("Can't convert property {} value {} to a long, using default {}", propertyName, property, defaultValue, ex);
        }
      }
      logger.debug("System property {}'s value is {}", propertyName, result);

      return result;
    }

    protected abstract T valueOf(String property);
  }

  private static final Logger logger = LoggerFactory.getLogger(PropertiesHelper.class);
}
