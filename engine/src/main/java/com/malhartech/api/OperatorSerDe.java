/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Interface to control operator serialization.
 * Serialization of operators occurs as part of association to a DAG.
 */
public interface OperatorSerDe
{
  /**
   * Read object from stream.
   *
   * @param is stream of serialized object representation
   * @return java object
   */
  public Object read(InputStream is) throws IOException;

  /**
   * Write object to stream, without closing the stream.
   *
   * @param object instance to serialize
   * @param os stream to write to
   */
  public void write(Object object, OutputStream os) throws IOException;
}
