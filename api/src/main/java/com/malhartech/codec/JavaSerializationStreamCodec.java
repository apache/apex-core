/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.codec;

import com.malhartech.api.StreamCodec;
import com.malhartech.common.Fragment;
import java.io.*;
import org.slf4j.LoggerFactory;

/**
 * This codec is used for serializing the objects of class which implements java.io.Serializable.
 * It's not optimized for speed and should be used as the last resort if you know that the slowness of
 * it is not going to prevent you from operating your application in realtime.
 * @param <T> Type of the object which gets serialized/deserialized using this codec.
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class JavaSerializationStreamCodec<T extends Serializable> implements StreamCodec<T>
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(JavaSerializationStreamCodec.class);

  @Override
  public Object fromByteArray(DataStatePair dspair)
  {
    ByteArrayInputStream bis = new ByteArrayInputStream(dspair.data.buffer, dspair.data.offset, dspair.data.length);
    try {
      ObjectInputStream ois = new ObjectInputStream(bis);
      return ois.readObject();
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
      ObjectOutputStream oos = new ObjectOutputStream(bos);
      oos.writeObject(o);
      oos.flush();
      DataStatePair dsp = new DataStatePair();
      byte[] buffer = bos.toByteArray();
      dsp.data = new Fragment(buffer, 0, buffer.length);
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
    /* Java serialization is one of the slowest serialization techniques. */
  }

}
