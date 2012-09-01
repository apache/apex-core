/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.dag;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Interface to control node serialization. Nodes state
 * is externalized for redundancy or transfer in general.
 */
public interface NodeSerDe
{
  /**
   * Read POJO from stream.
   *
   * @param is stream of serialized representation of the object
   * @return java object
   */
  public Object read(InputStream is) throws IOException;

  /**
   * Write POJO to stream, without closing the stream.
   *
   * @param object instance to serialize
   * @param os stream to write to
   * @return serialized representation of the object
   */
  public void write(Object object, OutputStream os) throws IOException;
}
