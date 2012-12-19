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
 * operators to share the load as per the partitions. The logic to correspond about
 * partitions is abstracted out in StreamCodec which is defined on each stream.<br>
 * <br>
 * The default StreamCodec does not define any partitions so it cannot be used for sticky
 * partitions. It can still do load balancing using Round Robin, Least Connection etc.<br>
 * <br>
 * Since stream has upstream operator and downstream operator which can emit and consume different
 * type of objects, the objects values associated with fromByteArray and toByteArray
 * could differ. In most cases they would be identical and is recommended to keep them
 * that way.<br>
 * <br>
 *
 * @author chetan
 */
public interface StreamCodec<T>
{
  public class DataStatePair
  {
    public byte[] data;
    public byte[] state;
  }

  /**
   * Create POJO from the byte array for consumption by the downstream.
   *
   * @param dspair
   * @return T (plain old java object)
   */
  T fromByteArray(DataStatePair dspair);

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
  int getPartition(Object o);

  /**
   * Do consolidation at the checkpoint. If the codec builds the state through its lifetime as it
   * processes the objects for serialization and deserialization, this is the point where it can
   * reset the state to the default state.
   */
  public void checkpoint();
}
