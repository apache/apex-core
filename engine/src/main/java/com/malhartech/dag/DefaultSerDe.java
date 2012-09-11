/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.dag;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.malhartech.annotation.ShipContainingJars;

import java.util.Collection;

/**
 *
 * Default SerDe for streams if nothing is configured. Has no partitioning<p>
 * <br>
 * No partitioning is done and it uses Kryo serializer for serde<br>
 * <br>
 *
 * Requires kryo and its dependencies in deployment
 */
@ShipContainingJars (classes={Kryo.class, org.objenesis.instantiator.ObjectInstantiator.class, com.esotericsoftware.minlog.Log.class, com.esotericsoftware.reflectasm.ConstructorAccess.class})
public class DefaultSerDe implements SerDe
{
  //private transient static final Logger logger = LoggerFactory.getLogger(DefaultSerDe.class);

  private Kryo kryo = new Kryo();
  private Output output = new Output(new byte[4096]);
  private Input input = new Input();

  @Override
  public Object fromByteArray(byte[] bytes)
  {
    input.setBuffer(bytes);
    return kryo.readClassAndObject(input);
  }

  @Override
  public byte[] toByteArray(Object o)
  {
    output.setPosition(0);
    kryo.writeClassAndObject(output, o);
    return output.toBytes();
  }

  @Override
  public byte[] getPartition(Object o)
  {
    return null;
  }

  @Override
  public byte[][] getPartitions() {
    return null;
  }

  @Override
  public boolean transferState(Node destination, Node source, Collection<byte[]> partitions)
  {
    return false;
  }

}