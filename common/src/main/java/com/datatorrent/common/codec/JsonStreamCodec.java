/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.common.codec;

import java.io.*;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;
import org.codehaus.jackson.map.ser.std.RawSerializer;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.StringCodec;
import com.datatorrent.common.util.ObjectMapperString;
import com.datatorrent.netlet.util.Slice;
import java.util.Map;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.ser.std.SerializerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @param <T> tuple type
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class JsonStreamCodec<T> implements StreamCodec<T>
{
  private ObjectMapper mapper;

  public JsonStreamCodec()
  {
    mapper = new ObjectMapper();
    mapper.configure(JsonGenerator.Feature.WRITE_NUMBERS_AS_STRINGS, true);
    SimpleModule module = new SimpleModule("MyModule", new Version(1, 0, 0, null));
    module.addSerializer(ObjectMapperString.class, new RawSerializer<Object>(Object.class));
    mapper.registerModule(module);
  }

  public JsonStreamCodec(Map<Class<?>, Class<? extends StringCodec<?>>> codecs)
  {
    mapper = new ObjectMapper();
    mapper.configure(JsonGenerator.Feature.WRITE_NUMBERS_AS_STRINGS, true);
    mapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
    SimpleModule module = new SimpleModule("MyModule", new Version(1, 0, 0, null));
    module.addSerializer(ObjectMapperString.class, new RawSerializer<Object>(Object.class));
    if (codecs != null) {
      for (Map.Entry<Class<?>, Class<? extends StringCodec<?>>> entry: codecs.entrySet()) {
        try {
          @SuppressWarnings("unchecked")
          final StringCodec<Object> codec = (StringCodec<Object>)entry.getValue().newInstance();
          module.addSerializer(new SerializerBase(entry.getKey())
          {
            @Override
            public void serialize(Object value, JsonGenerator jgen, SerializerProvider provider) throws IOException
            {
              jgen.writeString(codec.toString(value));
            }

          });
        }
        catch (Exception ex) {
          logger.error("Caught exception when instantiating codec for class {}", entry.getKey().getName(), ex);
        }
      }
    }
    mapper.registerModule(module);
  }

  @Override
  public Object fromByteArray(Slice data)
  {
    ByteArrayInputStream bis = new ByteArrayInputStream(data.buffer, data.offset, data.length);
    try {
      return mapper.readValue(bis, Object.class);
    }
    catch (Exception ioe) {
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
    }
    catch (IOException ex) {
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
