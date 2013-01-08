/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.api;


/**
 *
 * Serializing and Deserializing the data tuples and controlling the partitioning<p>
 * <br>
 * Data flows from one Operator to another Operator through a stream. For load balancing the
 * downstream operators, we can use sticky partitions. Since the framework has no knowledge
 * about the internals of the data flowing between operators, it has to ask the
 * application if payload can be partitioned and appropriately creates downstream
 * operators to share the load as per the partitions. This functionality is abstracted
 * out in StreamCodec. Typically StreamCodec is defined on each input stream and is able to
 * serialize/deserialize and partition the data of type supported by the stream.
 * <br /><br />
 * For a few known types, the system is able to determine the StreamCodec. In all other cases,
 * it would need user to define the codec on each input stream.
 * <br /> <br />
 * In the physical layout, each codec has at least 2 instances - serializer instance which
 * attaches itself to the stream coming out of upstream operator and deserializer instance
 * which attaches itself to input stream of downstream operator.
 *
 * @param <T> data type of the tuples on the stream
 * @author chetan
 */
public interface StreamCodec<T>
{
  /**
   * A convenience class which is used to hold 2 different values associated with each serialize/deserialize operation.
   */
  public class DataStatePair
  {
    /**
     * This byte array corresponds to serialized form of the tuple of type T.
     */
    public byte[] data;
    /**
     * This byte array corresponds to serialized form the incremental state the
     * codec built while serializing the tuple into data field.
     *
     * Since serialization and deserialization fall among the most expensive calculations
     * that a system can do, codecs are expected optimize these operations considerably.
     * An example of it is instead of storing fully qualified class name along with the
     * object, the codec can store an integer for it. This creates a mapping from an integer
     * to fully qualified class name and is only known to the codec instance which
     * serializes the object using this information. We call this dynamically changing
     * knowledge (mapping) a state. For deserializer instance to do its job, it also needs
     * to have this knowledge and can be passed through the state variable.
     *
     * Note that the state is incremental so it's additive to previous state. It does not
     * replace it. If state does not change during serialization, this field can be set to
     * null. Otherwise it will be delivered to all the instances of deserializing codecs of
     * this serializer in the same order as it was created. Due to the nature of the partitioning
     * the accompanying data field may not make it to the deserializer.
     */
    public byte[] state;
  }

  /**
   * Create POJO from the byte array for consumption by the downstream.
   *
   * @param dspair
   * @return plain old java object, the type is intentionally not T since the consumer does not care about it.
   */
  Object fromByteArray(DataStatePair dspair);

  /**
   * Serialize the POJO emitted by the upstream node to byte array so that
   * it can be transmitted or stored in file
   *
   * @param o plain old java object
   * @return serialized representation of the object
   */
  DataStatePair toByteArray(T o);

  /**
   * Get the partition on the object to be delivered to the downstream
   * so that it can be sent to appropriate downstream node if the load
   * balancing per partition is in effect.
   *
   * @param o object for which the partition has to be determined
   * @return byte array representing the partition for the object
   */
  int getPartition(T o);

  /**
   * Reset the state of the codec to the default state before any of the tuples are processed.
   *
   * The state used to serialize/deserialize after resetState is the same as the state codec has after
   * it is instantiated. resetState is called periodically when the upstream operator checkpoints but
   * should not be confused with the resetState operation of upstream operator.
   *
   */
  public void resetState();
}
