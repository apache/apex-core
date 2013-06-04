/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api.codec;

import java.io.*;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;
import org.codehaus.jackson.map.ser.impl.RawSerializer;

import com.malhartech.api.StreamCodec;
import com.malhartech.api.util.ObjectMapperString;
import com.malhartech.common.util.Slice;

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

}
