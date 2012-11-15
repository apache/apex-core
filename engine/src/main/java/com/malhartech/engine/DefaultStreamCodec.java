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
import com.esotericsoftware.kryo.util.ObjectMap;
import com.esotericsoftware.kryo.util.Util;
import com.malhartech.annotation.ShipContainingJars;
import com.malhartech.api.Operator;
import com.malhartech.api.StreamCodec;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private Output output = new Output(4096, Integer.MAX_VALUE);
  private Input input = new Input();

  static class ClassIdPair
  {
    final int id;
    final String type;

    ClassIdPair()
    {
      id = 0;
      type = null;
    }

    public ClassIdPair(int id, String type)
    {
      this.id = id;
      this.type = type;
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
  public Object fromByteArray(byte[] bytes)
  {
    input.setBuffer(bytes);
    Object o = readClassAndObject(input);
    if (o instanceof ClassIdPair) {
      try {
        do {
          ClassIdPair pair = (ClassIdPair)o;
          register(Class.forName(pair.type, false, getClassLoader()), pair.id);
          o = readClassAndObject(input);
        }
        while (o instanceof ClassIdPair);
      }
      catch (ClassNotFoundException ex) {
        logger.debug("exception", ex);
        o = null;
      }
    }
    return o;
  }

  @Override
  public byte[] toByteArray(Object o)
  {
    output.setPosition(0);
    writeClassAndObject(output, o);

    if (!pairs.isEmpty()) {
      final Output out = new Output(4096, Integer.MAX_VALUE);
      for (ClassIdPair cip: pairs) {
        writeClassAndObject(out, cip);
      }
      pairs.clear();

      out.write(output.getBuffer(), 0, output.position());
      return out.toBytes();
    }

    return output.toBytes();
  }

  @Override
  public byte[] getPartition(Object o)
  {
    return null;
  }

  @Override
  public byte[][] getPartitions()
  {
    return null;
  }

  @Override
  public boolean transferState(Operator destination, Operator source, Collection<byte[]> partitions)
  {
    return false;
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
      else if (!type.isEnum() && Enum.class.isAssignableFrom(type)) {
        // This handles an enum value that is an inner class. Eg: enum A {b{}};
        registration = getRegistration(type.getEnclosingClass());
      }
      else if (EnumSet.class.isAssignableFrom(type)) {
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