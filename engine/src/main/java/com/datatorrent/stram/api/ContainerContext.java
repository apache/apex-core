/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
  public static final Attribute<String> IDENTIFIER = new Attribute<String>("unknown_container_id");
  public static final Attribute<Integer> BUFFER_SERVER_MB = new Attribute<Integer>(8*64);
  public static final Attribute<byte[]> BUFFER_SERVER_TOKEN = new Attribute<byte[]>(null, null);
  public static final Attribute<RequestFactory> REQUEST_FACTORY = new Attribute<RequestFactory>(null, null);
  @SuppressWarnings("FieldNameHidesFieldInSuperclass")
  long serialVersionUID = AttributeInitializer.initialize(ContainerContext.class);
}
