/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.common.experimental;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * Interface for App Data support. Experimental only. This interface will likely change in the near future.
 */
@InterfaceStability.Evolving
public interface AppData
{
  /**
   * This interface should be implemented by AppData Query and Result operators.
   */
  interface ConnectionInfoProvider
  {
    /**
     * Returns the connection url used by the appdata Query or Result operator.
     * @return The connection url used by the AppData Query or Result operator.
     */
    public String getAppDataURL();
    /**
     * Returns the topic that the appdata Query or Result operator sends data to.
     * @return The topic that the appdata Query or Result operator sends data to.
     */
    public String getTopic();
  }

  /**
   * Marker annotation on a result operator which indicates that the query id is appended to topic.
   */
  @Documented
  @Target(ElementType.TYPE)
  @Retention(RetentionPolicy.RUNTIME)
  @Inherited
  public @interface AppendQueryIdToTopic{boolean value() default false;}

  /**
   * Marker annotation for specifying appdata query ports.
   */
  @Documented
  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface QueryPort
  {
  }

  /**
   * Marker annotation for specifying appdata result ports.
   */
  @Documented
  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface ResultPort
  {
  }
}
