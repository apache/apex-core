/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.policy.GiveAll;
import com.malhartech.bufferserver.policy.Policy;
import com.malhartech.bufferserver.util.SerializedData;
import io.netty.channel.Channel;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LogicalNode represents a logical node in a DAG<p>
 * <br>
 * Logical node can be split into multiple physical nodes. The type of the logical node groups the multiple
 * physical nodes together in a group.
 * <br>
 *
 * @author chetan
 */
public class LogicalNode implements DataListener
{
  private static final Logger logger = LoggerFactory.getLogger(LogicalNode.class);
  private final String upstream;
  private final String group;
  private final HashSet<PhysicalNode> physicalNodes;
  private final HashSet<ByteBuffer> partitions;
  private final Policy policy;
  private final DataListIterator iterator;
  private final long windowId;
  private long baseSeconds;
  private boolean caughtup;

  /**
   *
   * @param upstream
   * @param group
   * @param iterator
   * @param policy
   */
  LogicalNode(String upstream, String group, Iterator<SerializedData> iterator, Policy policy, long startingWindowId)
  {
    this.upstream = upstream;
    this.group = group;
    this.policy = policy;
    this.physicalNodes = new HashSet<PhysicalNode>();
    this.partitions = new HashSet<ByteBuffer>();

    if (iterator instanceof DataListIterator) {
      this.iterator = (DataListIterator)iterator;
    }
    else {
      throw new IllegalArgumentException("iterator does not belong to DataListIterator class");
    }

    windowId = startingWindowId;
  }

  /**
   *
   * @return String
   */
  public String getGroup()
  {
    return group;
  }

  /**
   *
   * @return Iterator<SerializedData>
   */
  public Iterator<SerializedData> getIterator()
  {
    return iterator;
  }

  /**
   *
   * @param channel
   */
  public void addChannel(Channel channel)
  {
    PhysicalNode pn = new PhysicalNode(channel);
    if (!physicalNodes.contains(pn)) {
      physicalNodes.add(pn);
    }
  }

  /**
   *
   * @param channel
   */
  public void removeChannel(Channel channel)
  {
    for (PhysicalNode pn: physicalNodes) {
      if (pn.getChannel() == channel) {
        physicalNodes.remove(pn);
        break;
      }
    }
  }

  /**
   *
   * @param partition
   */
  public void addPartition(ByteBuffer partition)
  {
    partitions.add(partition);
  }

  // make it run a lot faster by tracking faster!
  /**
   *
   * @param windowId
   */
  public synchronized void catchUp()
  {
    int intervalMillis;

//    logger.debug("catching up {}->{}", upstream, group);
    /*
     * fast forward to catch up with the windowId without consuming
     */
    outer:
    while (iterator.hasNext()) {
      SerializedData data = iterator.next();
      switch (iterator.getType()) {
        case RESET_WINDOW:
          Data resetWindow = (Data)iterator.getData();
          baseSeconds = (long)resetWindow.getWindowId() << 32;
          intervalMillis = resetWindow.getResetWindow().getWidth();
          if (intervalMillis <= 0) {
            logger.warn("Interval value set to non positive value = {}", intervalMillis);
          }
          GiveAll.getInstance().distribute(physicalNodes, data);
          break;

        case BEGIN_WINDOW:
//          logger.debug("{}->{} condition {} =? {}", new Object[] {upstream, group, (baseSeconds | iterator.getWindowId()), windowId});
          if ((baseSeconds | iterator.getWindowId()) >= windowId) {
            GiveAll.getInstance().distribute(physicalNodes, data);
            caughtup = true;
            break outer;
          }
          break;

        case CHECKPOINT:
        case CODEC_STATE:
          GiveAll.getInstance().distribute(physicalNodes, data);
          break;
      }
    }

    if (iterator.hasNext()) {
      dataAdded(DataListener.NULL_PARTITION);
    }
  }

  /**
   *
   * @param partition
   */
  @SuppressWarnings("fallthrough")
  public synchronized void dataAdded(ByteBuffer partition)
  {
    if (caughtup) {
//      logger.debug("caught up {}->{}", upstream, group);
      /*
       * consume as much data as you can before running out of steam
       */
      if (partitions.isEmpty()) {
        while (iterator.hasNext()) {
          SerializedData data = iterator.next();
          switch (iterator.getType()) {
            case PARTITIONED_DATA:
            case SIMPLE_DATA:
              policy.distribute(physicalNodes, data);
              break;

            case NO_DATA:
              break;

            case RESET_WINDOW:
              Data resetWindow = (Data)iterator.getData();
              baseSeconds = (long)resetWindow.getWindowId() << 32;

            default:
              GiveAll.getInstance().distribute(physicalNodes, data);
              break;
          }
        }
      }
      else {
        while (iterator.hasNext()) {
          SerializedData data = iterator.next();
          switch (iterator.getType()) {
            case PARTITIONED_DATA:
              if (partitions.contains(((Data)iterator.getData()).getPartitionedData().getPartition().asReadOnlyByteBuffer())) {
                policy.distribute(physicalNodes, data);
              }
              break;

            case NO_DATA:
            case SIMPLE_DATA:
              break;

            case RESET_WINDOW:
              Data resetWindow = (Data)iterator.getData();
              baseSeconds = (long)resetWindow.getWindowId() << 32;
              break;

            default:
              GiveAll.getInstance().distribute(physicalNodes, data);
              break;
          }
        }
      }
    }
    else {
      catchUp();
    }
  }

  /**
   *
   * @param partitions
   * @return int
   */
  public int getPartitions(Collection<ByteBuffer> partitions)
  {
    partitions.addAll(this.partitions);
    return partitions.size();
  }

  /**
   *
   * @return int
   */
  public final int getPhysicalNodeCount()
  {
    return physicalNodes.size();
  }

  /**
   * @return the upstream
   */
  public String getUpstream()
  {
    return upstream;
  }

  public void setBaseSeconds(int baseSeconds)
  {
    this.baseSeconds = (long)baseSeconds << 32;
  }
}
