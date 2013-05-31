/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.codec;

import java.io.*;

import org.slf4j.LoggerFactory;

import com.malhartech.api.StreamCodec;
import com.malhartech.common.Fragment;

/**
 * This codec is used for serializing the objects of class which implements java.io.Serializable.
 * It's not optimized for speed and should be used as the last resort if you know that the slowness of
 * it is not going to prevent you from operating your application in realtime.
 *
 * @param <T> Type of the object which gets serialized/deserialized using this codec.
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class JavaSerializationStreamCodec<T extends Serializable> implements StreamCodec<T>
{
  @Override
  public Object fromByteArray(Fragment fragment)
  {
    ByteArrayInputStream bis = new ByteArrayInputStream(fragment.buffer, fragment.offset, fragment.length);
    try {
      ObjectInputStream ois = new ObjectInputStream(bis);
      return ois.readObject();
    }
    catch (Exception ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public Fragment toByteArray(T object)
  {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      ObjectOutputStream oos = new ObjectOutputStream(bos);
      oos.writeObject(object);
      oos.flush();
      byte[] buffer = bos.toByteArray();
      return new Fragment(buffer, 0, buffer.length);
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

  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(JavaSerializationStreamCodec.class);
}
