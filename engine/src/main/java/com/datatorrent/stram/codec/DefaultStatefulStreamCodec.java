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
package com.datatorrent.stram.codec;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;

import com.datatorrent.netlet.util.Slice;

/**
 * Default implementation of the StreamCodec.
 *
 * This implementation is used no codec is partitioned on the input streams of the operator.
 * It uses Kryo to serialize and deserialize the tuples and uses the hashcode of the tuple
 * object to faciliate the partitions.
 *
 * Requires kryo and its dependencies in deployment
 *
 * @param <T>
 * @since 0.3.2
 */
public class DefaultStatefulStreamCodec<T> extends Kryo implements StatefulStreamCodec<T>
{
  private final Output data;
  private final Output state;
  private final Input input;
  private final DataStatePair dataStatePair;

  @SuppressWarnings("OverridableMethodCallInConstructor")
  public DefaultStatefulStreamCodec()
  {
    super(new ClassResolver(), new MapReferenceResolver());
    data = new Output(4096, Integer.MAX_VALUE);
    state = new Output(4096, Integer.MAX_VALUE);
    input = new Input();

    register(Class.class);
    register(ClassIdPair.class);
    classResolver = (ClassResolver)getClassResolver();
    this.pairs = classResolver.pairs;
    classResolver.init();
    dataStatePair = new DataStatePair();
  }

  @Override
  public T fromDataStatePair(DataStatePair dspair)
  {
    if (dspair.state != null) {
      try {
        input.setBuffer(dspair.state.buffer, dspair.state.offset, dspair.state.length);
        while (input.position() < input.limit()) {
          ClassIdPair pair = (ClassIdPair)readClassAndObject(input);
          classResolver.registerExplicit(pair);
        }
      } catch (Throwable th) {
        logger.error("Catastrophic Error: Execution halted due to Kryo exception!", th);
        synchronized (this) {
          try {
            wait();
          } catch (InterruptedException ex) {
            throw new RuntimeException("Serialization State Error Halt Interrupted", ex);
          }
        }
      } finally {
        dspair.state = null;
      }
    }

    input.setBuffer(dspair.data.buffer, dspair.data.offset, dspair.data.length);
    // the following code does not need to be in the try-catch block. It can be
    // taken out of it, once the stability of the code is validated by 4/1/2014.
    try {
      return (T)readClassAndObject(input);
    } catch (Throwable th) {
      logger.error("Catastrophic Error: Execution halted due to Kryo exception!", th);
      synchronized (this) {
        try {
          wait();
        } catch (InterruptedException ex) {
          throw new RuntimeException("Serialization Data Error Halt Interrupted", ex);
        }
      }
      return null;
    }
  }

  @Override
  public DataStatePair toDataStatePair(T o)
  {
    data.setPosition(0);
    writeClassAndObject(data, o);

    if (!pairs.isEmpty()) {
      state.setPosition(0);
      for (ClassIdPair cip : pairs) {
        writeClassAndObject(state, cip);
      }
      pairs.clear();

      dataStatePair.state = new Slice(state.getBuffer(), 0, state.position());
    } else {
      dataStatePair.state = null;
    }

    dataStatePair.data = new Slice(data.getBuffer(), 0, data.position());
    return dataStatePair;
  }

  @Override
  public int getPartition(T o)
  {
    return o.hashCode();
  }

  @Override
  public void resetState()
  {
    classResolver.unregisterImplicitlyRegisteredTypes();
  }

  final ClassResolver classResolver;
  final ArrayList<ClassIdPair> pairs;

  @Override
  public Object fromByteArray(Slice fragment)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Slice toByteArray(T o)
  {
    throw new UnsupportedOperationException();
  }

  static class ClassIdPair
  {
    final int id;
    final String classname;

    ClassIdPair()
    {
      id = 0;
      classname = null;
    }

    ClassIdPair(int id, String classname)
    {
      this.id = id;
      this.classname = classname;
    }

  }

  public static class ClassResolver extends DefaultClassResolver
  {
    int firstAvailableRegistrationId;
    int nextAvailableRegistrationId;
    final ArrayList<ClassIdPair> pairs = new ArrayList<>();

    public void unregister(int classId)
    {
      Registration registration = idToRegistration.remove(classId);
      classToRegistration.remove(registration.getType());
      getRegistration(int.class); /* make sure that we bust the memoized cache in superclass */
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Registration registerImplicit(Class type)
    {
      while (getRegistration(nextAvailableRegistrationId) != null) {
        nextAvailableRegistrationId++;
      }

      //logger.debug("adding new classid pair {} => {}", nextAvailableRegistrationId, type.getName());
      pairs.add(new ClassIdPair(nextAvailableRegistrationId, type.getName()));
      return register(new Registration(type, kryo.getDefaultSerializer(type), nextAvailableRegistrationId++));
    }

    public void registerExplicit(ClassIdPair pair) throws ClassNotFoundException
    {
      //logger.debug("registering class {} => {}", pair.classname, pair.id);
      //pairs.add(pair);
      Class type = Class.forName(pair.classname, false, Thread.currentThread().getContextClassLoader());
      register(new Registration(type, kryo.getDefaultSerializer(type), pair.id));
      if (nextAvailableRegistrationId <= pair.id) {
        nextAvailableRegistrationId = pair.id + 1;
      }
    }

    public void init()
    {
      firstAvailableRegistrationId = kryo.getNextRegistrationId();
      nextAvailableRegistrationId = firstAvailableRegistrationId;
    }

    public void unregisterImplicitlyRegisteredTypes()
    {
      while (nextAvailableRegistrationId > firstAvailableRegistrationId) {
        unregister(--nextAvailableRegistrationId);
      }
    }

  }

  @Override
  public DefaultStatefulStreamCodec<T> newInstance()
  {
    try {
      return getClass().newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException("Unable to create new stateful streamcodec object", e);
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(DefaultStatefulStreamCodec.class);
}
