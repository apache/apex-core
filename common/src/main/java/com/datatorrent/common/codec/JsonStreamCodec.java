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
package com.datatorrent.common.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.ser.std.SerializerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.StringCodec;
import com.datatorrent.common.util.JacksonObjectMapperProvider;
import com.datatorrent.netlet.util.Slice;

/**
 * <p>JsonStreamCodec class.</p>
 *
 * @param <T> tuple type
 * @since 3.0.0
 */
public class JsonStreamCodec<T> implements StreamCodec<T>
{
  private ObjectMapper mapper;

  public JsonStreamCodec()
  {
    mapper = new JacksonObjectMapperProvider().getContext(null);
  }

  public JsonStreamCodec(Map<Class<?>, Class<? extends StringCodec<?>>> codecs)
  {
    JacksonObjectMapperProvider jomp = new JacksonObjectMapperProvider();
    if (codecs != null) {
      for (Map.Entry<Class<?>, Class<? extends StringCodec<?>>> entry: codecs.entrySet()) {
        try {
          @SuppressWarnings("unchecked")
          final StringCodec<Object> codec = (StringCodec<Object>)entry.getValue().newInstance();
          jomp.addSerializer(new SerializerBase(entry.getKey())
          {
            @Override
            public void serialize(Object value, JsonGenerator jgen, SerializerProvider provider) throws IOException
            {
              jgen.writeString(codec.toString(value));
            }

          });
        } catch (Exception ex) {
          logger.error("Caught exception when instantiating codec for class {}", entry.getKey().getName(), ex);
        }
      }
    }
    mapper = jomp.getContext(null);
  }

  @Override
  public Object fromByteArray(Slice data)
  {
    ByteArrayInputStream bis = new ByteArrayInputStream(data.buffer, data.offset, data.length);
    try {
      return mapper.readValue(bis, Object.class);
    } catch (Exception ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public Slice toByteArray(T o)
  {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();

    try {
      mapper.writeValue(bos, o);
      byte[] bytes = bos.toByteArray();
      return new Slice(bytes, 0, bytes.length);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public int getPartition(T o)
  {
    return o.hashCode();
  }

  private static final Logger logger = LoggerFactory.getLogger(JsonStreamCodec.class);
}
