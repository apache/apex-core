/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.engine;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.EnumSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import com.malhartech.annotation.ShipContainingJars;
import com.malhartech.api.StreamCodec;

/**
 *
 * Default StreamCodec for streams if nothing is configured. Has no partitioning<p>
 * <br>
 * No partitioning is done and it uses Kryo serializer for serde<br>
 * <br>
 *
 * Requires kryo and its dependencies in deployment
 */
@ShipContainingJars(classes = {Kryo.class, org.objenesis.instantiator.ObjectInstantiator.class, com.esotericsoftware.minlog.Log.class, com.esotericsoftware.reflectasm.ConstructorAccess.class})
public class DefaultStreamCodec extends Kryo implements StreamCodec<Object>
{
  private static final Logger logger = LoggerFactory.getLogger(DefaultStreamCodec.class);
  private final Output data = new Output(4096, Integer.MAX_VALUE);
  private final Output state = new Output(4096, Integer.MAX_VALUE);
  private final Input input = new Input();

  static class ClassIdPair
  {
    final int id;
    final String classname;

    ClassIdPair()
    {
      id = 0;
      classname = null;
    }

    public ClassIdPair(int id, String classname)
    {
      this.id = id;
      this.classname = classname;
    }
  }

  static class ClassResolver extends DefaultClassResolver
  {
    int firstAvailableRegistrationId;
    int nextAvailableRegistrationId;
    final ArrayList<ClassIdPair> pairs = new ArrayList<ClassIdPair>();

    public void unregister(int classId)
    {
      Registration registration = idToRegistration.remove(classId);
      classToRegistration.remove(registration.getType());
    }

    @Override
    public Registration registerImplicit(Class type)
    {
      while (getRegistration(nextAvailableRegistrationId) != null) {
        nextAvailableRegistrationId++;
      }

      pairs.add(new ClassIdPair(nextAvailableRegistrationId, type.getName()));
      return register(new Registration(type, kryo.getDefaultSerializer(type), nextAvailableRegistrationId++));
    }

    public void init()
    {
      firstAvailableRegistrationId = kryo.getNextRegistrationId();
      nextAvailableRegistrationId = firstAvailableRegistrationId;
    }

    public void checkpoint()
    {
      for (int i = firstAvailableRegistrationId; i < nextAvailableRegistrationId; i++) {
        unregister(i);
      }

      nextAvailableRegistrationId = firstAvailableRegistrationId;
    }
  }

  @SuppressWarnings("OverridableMethodCallInConstructor")
  public DefaultStreamCodec()
  {
    super(new ClassResolver(), new MapReferenceResolver());
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
        input.setBuffer(dspair.state);
        while (input.position() < input.limit()) {
          ClassIdPair pair = (ClassIdPair)readClassAndObject(input);
          register(Class.forName(pair.classname, false, getClassLoader()), pair.id);
        }
      }
      catch (ClassNotFoundException ex) {
        logger.debug("exception", ex);
        throw new RuntimeException(ex);
      }
      finally {
        dspair.state = null;
      }
    }

    input.setBuffer(dspair.data);
    return readClassAndObject(input);
  }

  @Override
  public DataStatePair toByteArray(Object o)
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

      pair.state = state.toBytes();
    }

    pair.data = data.toBytes();
    return pair;
  }

  @Override
  public byte[] getPartition(Object o)
  {
    return null;
  }

  @Override
  public void checkpoint()
  {
    classResolver.checkpoint();
  }

  @Override
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
}