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
package com.datatorrent.common.experimental;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

/**
 * Interface for App Data support. Experimental only. This interface will likely change in the near future.
 *
 * @since 3.0.0
 */
@InterfaceStability.Evolving
public interface AppData
{
  /**
   * This interface is for App Data stores which support embedding a query operator.
   * @param <QUERY_TYPE> The type of the query tuple emitted by the embedded query operator.
   */
  interface Store<QUERY_TYPE> extends Operator.ActivationListener<OperatorContext>
  {
    /**
     * Gets the query connector which is used by the store operator to receive queries. If this method returns
     * null then this Store should have a separate query operator connected to it.
     * @return The query connector which is used by the store operator to receive queries.
     */
    EmbeddableQueryInfoProvider<QUERY_TYPE> getEmbeddableQueryInfoProvider();

    /**
     * Sets the query connector which is used by the store operator to receive queries. The store operator will call
     * the {@link EmbeddableQueryInfoProvider#enableEmbeddedMode} method of the embeddable query operator before
     * its {@link Operator#setup} method is called.
     * @param embeddableQueryInfoProvider The query connector which is used by the store operator to receive queries.
     */
    void setEmbeddableQueryInfoProvider(EmbeddableQueryInfoProvider<QUERY_TYPE> embeddableQueryInfoProvider);
  }

  /**
   * This interface represents a query operator which can be embedded into an AppData data source. This operator could also
   * be used as a standalone operator. The distinction between being used in a standalone or embedded context is made by
   * the {@link EmbeddableQueryInfoProvider#enableEmbeddedMode} method. If this method is called at least once then the {@link EmbeddableQueryInfoProvider}
   * will operate as if it were embedded in an {@link AppData.Store} operator. If this method is never called then the operator will behave as if
   * it were a standalone operator.<br/><br/>
   * <b>Note:</b> When an {@link EmbeddableQueryInfoProvider} is set on an {@link AppData.Store} then it's {@link EmbeddableQueryInfoProvider#enableEmbeddedMode}
   * method is called before {@link Operator#setup}.
   * @param <QUERY_TYPE> The type of the query emitted by the operator.
   */
  interface EmbeddableQueryInfoProvider<QUERY_TYPE> extends Operator, ConnectionInfoProvider, Operator.ActivationListener<OperatorContext>
  {
    /**
     * Gets the output port for queries.
     * @return The output port for queries.
     */
    DefaultOutputPort<QUERY_TYPE> getOutputPort();

    /**
     * If this method is called at least once then this operator will work as if it were embedded in an {@link AppData.Store}.
     * If this method is never called then this operator will behave as a standalone operator. When an {@link EmbeddableQueryInfoProvider}
     * is set on an {@link AppData.Store} then the {@link AppData.Store} will call the {@link EmbeddableQueryInfoProvider#enableEmbeddedMode}
     * method once before the {@link Operator.setup} is called.
     */
    void enableEmbeddedMode();
  }

  /**
   * This interface should be implemented by AppData Query and Result operators.
   */
  interface ConnectionInfoProvider
  {
    /**
     * Returns the connection url used by the appdata Query or Result operator.
     * @return The connection url used by the AppData Query or Result operator.
     */
    String getAppDataURL();

    /**
     * Returns the topic that the appdata Query or Result operator sends data to.
     * @return The topic that the appdata Query or Result operator sends data to.
     */
    String getTopic();
  }

  /**
   * Marker annotation on a result operator which indicates that the query id is appended to topic.
   */
  @Documented
  @Target(ElementType.TYPE)
  @Retention(RetentionPolicy.RUNTIME)
  @Inherited
  @interface AppendQueryIdToTopic
  {
    boolean value() default false;
  }

  /**
   * Marker annotation for specifying appdata query ports.
   */
  @Documented
  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  @interface QueryPort
  {
  }

  /**
   * Marker annotation for specifying appdata result ports.
   */
  @Documented
  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  @interface ResultPort
  {
  }
}
