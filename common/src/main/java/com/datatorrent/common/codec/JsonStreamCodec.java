/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.malhartech.api.StreamCodec;
import com.malhartech.util.ObjectMapperString;
import java.io.*;
import com.malhartech.common.Fragment;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;
import org.codehaus.jackson.map.ser.impl.RawSerializer;

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
  public Object fromByteArray(DataStatePair dspair)
  {
    ByteArrayInputStream bis = new ByteArrayInputStream(dspair.data.buffer, dspair.data.offset, dspair.data.length);
    try {
      return mapper.readValue(bis, Object.class);
    }
    catch (Exception ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public DataStatePair toByteArray(T o)
  {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();

    try {
      mapper.writeValue(bos, o);
      DataStatePair dsp = new DataStatePair();
      byte[] bytes = bos.toByteArray();
      dsp.data = new Fragment(bytes, 0, bytes.length);
      return dsp;
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

  @Override
  public void resetState()
  {
  }

}
