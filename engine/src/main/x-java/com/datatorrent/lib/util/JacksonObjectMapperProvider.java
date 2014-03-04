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
package com.datatorrent.lib.util;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.module.SimpleModule;
import org.codehaus.jackson.map.ser.std.RawSerializer;

/**
 * <p>JacksonObjectMapperProvider class.</p>
 *
 * @since 0.3.2
 */
@Provider
@Produces(MediaType.APPLICATION_JSON)
public class JacksonObjectMapperProvider implements ContextResolver<ObjectMapper>
{
  private final ObjectMapper objectMapper;

  /**
   * <p>Constructor for JacksonObjectMapperProvider.</p>
   */
  public JacksonObjectMapperProvider()
  {
    this.objectMapper = new ObjectMapper();
    objectMapper.configure(JsonGenerator.Feature.WRITE_NUMBERS_AS_STRINGS, true);
    objectMapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
    SimpleModule module = new SimpleModule("MyModule", new Version(1, 0, 0, null));
    module.addSerializer(ObjectMapperString.class, new RawSerializer<Object>(Object.class));
    objectMapper.registerModule(module);
  }

  /** {@inheritDoc}
   * @param type
   * @return
   */
  @Override
  public ObjectMapper getContext(Class<?> type)
  {
    return objectMapper;
  }

}
