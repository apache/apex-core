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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.common.util.SerializableObject;
import com.datatorrent.netlet.util.DTThrowable;
import com.datatorrent.netlet.util.Slice;

/**
 * This codec is used for serializing the objects of class which are Kryo
 * serializable. Used for stream codec wrapper used for persistence
 *
 * @since 3.2.0
 */
public class DefaultKryoStreamCodec<T> extends SerializableObject implements StreamCodec<T>
{
  final static Logger logger = LoggerFactory.getLogger(DefaultKryoStreamCodec.class);

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
    try {
      ByteArrayInputStream is = new ByteArrayInputStream(fragment.buffer, fragment.offset, fragment.length);
      Input input = new Input(is);
      Object returnObject = kryo.readClassAndObject(input);
      is.close();
      return returnObject;
    } catch (IOException e) {
      DTThrowable.wrapIfChecked(e);
    }
    return null;
  }

  @Override
  public Slice toByteArray(T info)
  {
    Slice slice = null;
    try {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      Output output = new Output(os);
      kryo.writeClassAndObject(output, info);
      output.flush();
      slice = new Slice(os.toByteArray(), 0, os.toByteArray().length);
      os.close();
    } catch (IOException e) {
      DTThrowable.wrapIfChecked(e);
    }
    return slice;
  }

  @Override
  public int getPartition(T t)
  {
    return t.hashCode();
  }
}
