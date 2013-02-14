/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.Buffer.Message;
import com.malhartech.bufferserver.policy.GiveAll;
import com.malhartech.bufferserver.policy.Policy;
import com.malhartech.bufferserver.util.BitVector;
import com.malhartech.bufferserver.util.Codec;
import com.malhartech.bufferserver.util.SerializedData;
import io.netty.channel.Channel;
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
  private final HashSet<BitVector> partitions;
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
    this.partitions = new HashSet<BitVector>();

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
  public synchronized void addChannel(Channel channel)
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
  public synchronized void removeChannel(Channel channel)
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
   * @param mask
   */
  public synchronized void addPartition(int partition, int mask)
  {
    partitions.add(new BitVector(partition, mask));
  }

  // make it run a lot faster by tracking faster!
  /**
   *
   */
  public synchronized void catchUp()
  {
    int intervalMillis;

//    logger.debug("catching up {}->{}", upstream, group);

    try {
      /*
       * fast forward to catch up with the windowId without consuming
       */
      outer:
      while (iterator.hasNext()) {
        SerializedData data = iterator.next();
        switch (iterator.getType()) {
          case RESET_WINDOW:
            Message resetWindow = (Message)iterator.getData();
            baseSeconds = (long)resetWindow.getResetWindow().getBaseSeconds() << 32;
            intervalMillis = resetWindow.getResetWindow().getWidth();
            if (intervalMillis <= 0) {
              logger.warn("Interval value set to non positive value = {}", intervalMillis);
            }
            GiveAll.getInstance().distribute(physicalNodes, data);
            break;

          case BEGIN_WINDOW:
            logger.debug("{}->{} condition {} =? {}",
                         new Object[] {
                      upstream,
                      group,
                      Codec.getStringWindowId(baseSeconds | iterator.getWindowId()),
                      Codec.getStringWindowId(windowId)
                    });
            if ((baseSeconds | iterator.getWindowId()) >= windowId) {
              logger.debug("caught up {}->{}", upstream, group);
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
    }
    catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }

    if (iterator.hasNext()) {
      dataAdded();
    }
  }

  /**
   *
   */
  @SuppressWarnings("fallthrough")
  public synchronized void dataAdded()
  {
    if (caughtup) {
      try {
        /*
         * consume as much data as you can before running out of steam
         */
        if (partitions.isEmpty()) {
          while (iterator.hasNext()) {
            SerializedData data = iterator.next();
            switch (iterator.getType()) {
              case PAYLOAD:
                policy.distribute(physicalNodes, data);
                break;

              case NO_MESSAGE:
              case NO_MESSAGE_ODD:
                break;

              case RESET_WINDOW:
                Message resetWindow = (Message)iterator.getData();
                baseSeconds = (long)resetWindow.getResetWindow().getBaseSeconds() << 32;

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
              case PAYLOAD:
                int value = ((Message)iterator.getData()).getPayload().getPartition();
                for (BitVector bv: partitions) {
                  if (bv.matches(value)) {
                    policy.distribute(physicalNodes, data);
                    break;
                  }
                }
                break;

              case NO_MESSAGE:
              case NO_MESSAGE_ODD:
                break;

              case RESET_WINDOW:
                Message resetWindow = (Message)iterator.getData();
                baseSeconds = (long)resetWindow.getResetWindow().getBaseSeconds() << 32;

              default:
                GiveAll.getInstance().distribute(physicalNodes, data);
                break;
            }
          }
        }
      }
      catch (InterruptedException ie) {
        throw new RuntimeException(ie);
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
  public int getPartitions(Collection<BitVector> partitions)
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

}
