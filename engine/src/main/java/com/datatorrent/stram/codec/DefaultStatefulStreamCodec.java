/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.codec;

import java.util.ArrayList;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.datatorrent.common.util.Slice;

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
  }

  @Override
  public Object fromDataStatePair(DataStatePair dspair)
  {
    if (dspair.state != null) {
      try {
        input.setBuffer(dspair.state.buffer, dspair.state.offset, dspair.state.length);
        while (input.position() < input.limit()) {
          ClassIdPair pair = (ClassIdPair)readClassAndObject(input);
          classResolver.registerExplicit(pair);
        }
      }
      catch (Throwable th) {
        logger.error("Catastrophic Error: Execution halted due to Kryo exception!", th);
        synchronized (this) {
          try {
            wait();
          }
          catch (InterruptedException ex) {
            throw new RuntimeException("Serialization State Error Halt Interrupted", ex);
          }
        }
      }
      finally {
        dspair.state = null;
      }
    }

    input.setBuffer(dspair.data.buffer, dspair.data.offset, dspair.data.length);
    // the following code does not need to be in the try-catch block. It can be
    // taken out of it, once the stability of the code is validated by 4/1/2014.
    try {
      return readClassAndObject(input);
    }
    catch (Throwable th) {
      logger.error("Catastrophic Error: Execution halted due to Kryo exception!", th);
      synchronized (this) {
        try {
          wait();
        }
        catch (InterruptedException ex) {
          throw new RuntimeException("Serialization Data Error Halt Interrupted", ex);
        }
      }
      return null;
    }
  }

  @Override
  public DataStatePair toDataStatePair(T o)
  {
    DataStatePair pair = new DataStatePair();
    data.setPosition(0);
    writeClassAndObject(data, o);

    if (!pairs.isEmpty()) {
      state.setPosition(0);
      for (ClassIdPair cip : pairs) {
        writeClassAndObject(state, cip);
      }
      pairs.clear();

      // can we optimize this?
      byte[] bytes = state.toBytes();
      pair.state = new Slice(bytes, 0, bytes.length);
    }

    byte[] bytes = data.toBytes();
    pair.data = new Slice(bytes, 0, bytes.length);
    return pair;
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
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Slice toByteArray(T o)
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
    final ArrayList<ClassIdPair> pairs = new ArrayList<ClassIdPair>();

    public void unregister(int classId)
    {
      Registration registration = idToRegistration.remove(classId);
      classToRegistration.remove(registration.getType());
      getRegistration(int.class); /* make sure that we bust the memoized cache in superclass */
    }

    @Override
    @SuppressWarnings("rawtypes")
    public synchronized Registration registerImplicit(Class type)
    {
      while (getRegistration(nextAvailableRegistrationId) != null) {
        nextAvailableRegistrationId++;
      }

      //logger.debug("adding new classid pair {} => {}", nextAvailableRegistrationId, type.getName());
      pairs.add(new ClassIdPair(nextAvailableRegistrationId, type.getName()));
      return register(new Registration(type, kryo.getDefaultSerializer(type), nextAvailableRegistrationId++));
    }

    // Synchronizing with implicit registration as receive and send happen asynchronously
    public synchronized void registerExplicit(ClassIdPair pair) throws ClassNotFoundException
    {
      //logger.debug("registering class {} => {}", pair.classname, pair.id);
      pairs.add(pair);
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

  private static final Logger logger = LoggerFactory.getLogger(DefaultStatefulStreamCodec.class);
}
