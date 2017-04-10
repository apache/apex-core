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
package com.datatorrent.stram.api;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Attribute.AttributeMap.AttributeInitializer;
import com.datatorrent.api.Context;

/**
 * <p>ContainerContext interface.</p>
 *
 * @since 0.3.5
 */
public interface ContainerContext extends Context
{
  Attribute<String> IDENTIFIER = new Attribute<>("unknown_container_id");
  Attribute<Integer> BUFFER_SERVER_MB = new Attribute<>(8 * 64);
  Attribute<byte[]> BUFFER_SERVER_TOKEN = new Attribute<>(null, null);
  Attribute<RequestFactory> REQUEST_FACTORY = new Attribute<>(null, null);
  @SuppressWarnings("FieldNameHidesFieldInSuperclass")
  long serialVersionUID = AttributeInitializer.initialize(ContainerContext.class);
}
