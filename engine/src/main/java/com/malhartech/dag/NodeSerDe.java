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
   * @param is stream of serialized object representation
   * @return java object
   */
  public Object read(InputStream is) throws IOException;

  /**
   * Write POJO to stream, without closing the stream.
   *
   * @param object instance to serialize
   * @param os stream to write to
   */
  public void write(Object object, OutputStream os) throws IOException;
}
