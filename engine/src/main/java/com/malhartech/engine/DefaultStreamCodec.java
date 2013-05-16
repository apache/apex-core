/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.engine;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import com.malhartech.annotation.ShipContainingJars;
import com.malhartech.api.StreamCodec;
import com.malhartech.common.Fragment;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.EnumSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 */
@ShipContainingJars(classes = {Kryo.class, org.objenesis.instantiator.ObjectInstantiator.class, com.esotericsoftware.minlog.Log.class, com.esotericsoftware.reflectasm.ConstructorAccess.class})
public class DefaultStreamCodec<T> extends Kryo implements StreamCodec<T>
{
  private final Output data;
  private final Output state;
  private final Input input;

  @SuppressWarnings("OverridableMethodCallInConstructor")
  public DefaultStreamCodec()
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
  public Object fromByteArray(DataStatePair dspair)
  {
    if (dspair.state != null) {
      try {
        input.setBuffer(dspair.state.buffer, dspair.state.offset, dspair.state.offset + dspair.state.length);
        while (input.position() < input.limit()) {
          ClassIdPair pair = (ClassIdPair)readClassAndObject(input);
          //logger.debug("registering class {} => {}", pair.classname, pair.id);
          register(Class.forName(pair.classname, false, Thread.currentThread().getContextClassLoader()), pair.id);
        }
      }
      catch (Exception ex) {
        logger.error("Catastrophic Error: Execution halted due to Kryo exception!", ex);
        synchronized (this) {
          try {
            wait();
          }
          catch (Exception e) {
          }
        }
      }
      finally {
        dspair.state = null;
      }
    }

    input.setBuffer(dspair.data.buffer, dspair.data.offset, dspair.data.offset + dspair.data.length);
    try {
      return readClassAndObject(input);
    }
    catch (Exception ex) {
      logger.error("Catastrophic Error: Execution halted due to Kryo exception!", ex);
      synchronized (this) {
        try {
          wait();
        }
        catch (Exception e) {
        }
      }
      return null;
    }
  }

  @Override
  public DataStatePair toByteArray(T o)
  {
    DataStatePair pair = new DataStatePair();
    data.setPosition(0);
    writeClassAndObject(data, o);

    if (!pairs.isEmpty()) {
      state.setPosition(0);
      for (ClassIdPair cip: pairs) {
        writeClassAndObject(state, cip);
      }
      pairs.clear();

      // can we optimize this?
      byte[] bytes = state.toBytes();
      pair.state = new Fragment(bytes, 0, bytes.length);
    }

    byte[] bytes = data.toBytes();
    pair.data = new Fragment(bytes, 0, bytes.length);
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

  @Override
  @SuppressWarnings("rawtypes")
  public Registration getRegistration(Class type)
  {
    if (type == null) {
      throw new IllegalArgumentException("type cannot be null.");
    }

    Registration registration = classResolver.getRegistration(type);


    if (registration == null) {
      if (Proxy.isProxyClass(type)) {
        // If a Proxy class, treat it like an InvocationHandler because the concrete class for a proxy is generated.
        registration = getRegistration(InvocationHandler.class);
      }
      else if (!type.isEnum() && Enum.class
              .isAssignableFrom(type)) {
        // This handles an enum value that is an inner class. Eg: enum A {b{}};
        registration = getRegistration(type.getEnclosingClass());
      }
      else if (EnumSet.class
              .isAssignableFrom(type)) {
        registration = classResolver.getRegistration(EnumSet.class);
      }
      if (registration == null) {
        if (isRegistrationRequired()) {
          throw new IllegalArgumentException("Class is not registered: " + type.getName()
                  + "\nNote: To register this class use: kryo.register(" + type.getName() + ".class);");
        }
        registration = classResolver.registerImplicit(type);
      }
    }
    return registration;
  }

  final ClassResolver classResolver;
  final ArrayList<ClassIdPair> pairs;

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
    public Registration registerImplicit(Class type)
    {
      while (getRegistration(nextAvailableRegistrationId) != null) {
        nextAvailableRegistrationId++;
      }

      //logger.debug("adding new classid pair {} => {}", nextAvailableRegistrationId, type.getName());
      pairs.add(new ClassIdPair(nextAvailableRegistrationId, type.getName()));
      return register(new Registration(type, kryo.getDefaultSerializer(type), nextAvailableRegistrationId++));
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

  private static final Logger logger = LoggerFactory.getLogger(DefaultStreamCodec.class);
}