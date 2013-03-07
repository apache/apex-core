/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.malhartech.api.StreamCodec;
import java.io.*;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

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
  }

  @Override
  public Object fromByteArray(DataStatePair dspair)
  {
    ByteArrayInputStream bis = new ByteArrayInputStream(dspair.data);
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
      dsp.data = bos.toByteArray();
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
