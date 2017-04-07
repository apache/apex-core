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

import java.io.IOException;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.module.SimpleModule;
import org.codehaus.jackson.map.ser.std.RawSerializer;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

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
  private final SimpleModule module = new SimpleModule("MyModule", new Version(1, 0, 0, null));

  /**
   * <p>Constructor for JacksonObjectMapperProvider.</p>
   */
  public JacksonObjectMapperProvider()
  {
    this.objectMapper = new ObjectMapper();
    objectMapper.configure(JsonGenerator.Feature.WRITE_NUMBERS_AS_STRINGS, true);
    objectMapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
    module.addSerializer(ObjectMapperString.class, new RawSerializer<>(Object.class));
    module.addSerializer(JSONObject.class, new RawSerializer<>(Object.class));
    module.addSerializer(JSONArray.class, new RawSerializer<>(Object.class));
    objectMapper.registerModule(module);
  }

  /** {@inheritDoc}
   * @param type
   * @return An object mapper.
   */
  @Override
  public ObjectMapper getContext(Class<?> type)
  {
    return objectMapper;
  }

  public <T> void addSerializer(Class<T> clazz, JsonSerializer<T> serializer)
  {
    module.addSerializer(clazz, serializer);
  }

  public <T> void addSerializer(JsonSerializer<T> serializer)
  {
    module.addSerializer(serializer);
  }

  public JSONObject toJSONObject(Object o)
  {
    try {
      return new JSONObject(this.getContext(null).writeValueAsString(o));
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public <T> T fromJSONObject(JSONObject json, Class<T> clazz) throws IOException
  {
    return this.getContext(null).readValue(json.toString(), clazz);
  }

}
