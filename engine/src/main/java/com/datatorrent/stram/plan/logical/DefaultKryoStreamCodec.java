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
package com.datatorrent.stram.plan.logical;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.common.util.SerializableObject;
import com.datatorrent.netlet.util.Slice;

/**
 * This codec is used for serializing the objects of class which are Kryo
 * serializable. Used for stream codec wrapper used for persistence
 *
 * @since 3.2.0
 */
public class DefaultKryoStreamCodec<T> extends SerializableObject implements StreamCodec<T>
{
  private static final Logger logger = LoggerFactory.getLogger(DefaultKryoStreamCodec.class);

  private static final long serialVersionUID = 1L;
  protected final transient Kryo kryo;

  public DefaultKryoStreamCodec()
  {
    this.kryo = new Kryo();
    this.kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
  }

  @Override
  public Object fromByteArray(Slice fragment)
  {
    final Input input = new Input(fragment.buffer, fragment.offset, fragment.length);
    try {
      return kryo.readClassAndObject(input);
    } finally {
      input.close();
    }
  }

  @Override
  public Slice toByteArray(T o)
  {
    final Output output = new Output(32, -1);
    try {
      kryo.writeClassAndObject(output, o);
    } finally {
      output.close();
    }
    return new Slice(output.getBuffer(), 0, output.position());
  }

  @Override
  public int getPartition(T o)
  {
    return o.hashCode();
  }
}
