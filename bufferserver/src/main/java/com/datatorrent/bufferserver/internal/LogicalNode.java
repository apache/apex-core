/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.internal;

import com.datatorrent.bufferserver.client.AbstractClient;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.bufferserver.packet.Tuple;
import com.datatorrent.bufferserver.policy.GiveAll;
import com.datatorrent.bufferserver.policy.Policy;
import com.datatorrent.bufferserver.util.BitVector;
import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.bufferserver.util.SerializedData;
import com.datatorrent.netlet.EventLoop;

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
  private final String upstream;
  private final String group;
  private final HashSet<PhysicalNode> physicalNodes;
  private final HashSet<BitVector> partitions;
  private final Policy policy = GiveAll.getInstance();
  private final DataListIterator iterator;
  private final long skipWindowId;
  private long baseSeconds;
  private boolean caughtup;

  /**
   *
   * @param upstream
   * @param group
   * @param iterator
   * @param skipUptoWindowId
   */
  public LogicalNode(String upstream, String group, Iterator<SerializedData> iterator, long skipUptoWindowId)
  {
    this.upstream = upstream;
    this.group = group;
    this.physicalNodes = new HashSet<PhysicalNode>();
    this.partitions = new HashSet<BitVector>();

    if (iterator instanceof DataListIterator) {
      this.iterator = (DataListIterator)iterator;
    }
    else {
      throw new IllegalArgumentException("iterator does not belong to DataListIterator class");
    }

    skipWindowId = skipUptoWindowId;
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
   * @param connection
   */
  public void addConnection(AbstractClient connection)
  {
    PhysicalNode pn = new PhysicalNode(connection);
    if (!physicalNodes.contains(pn)) {
      physicalNodes.add(pn);
    }
  }

  /**
   *
   * @param client
   */
  public void removeChannel(AbstractClient client)
  {
    for (PhysicalNode pn: physicalNodes) {
      if (pn.getClient() == client) {
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
  public void addPartition(int partition, int mask)
  {
    partitions.add(new BitVector(partition, mask));
  }

  boolean ready = true;

  public boolean isReady()
  {
    if (!ready) {
      ready = true;
      for (PhysicalNode pn: physicalNodes) {
        if (pn.isBlocked()) {
          ready = pn.unblock() & ready;
        }
      }
    }

    return ready;
  }

  // make it run a lot faster by tracking faster!
  /**
   *
   */
  public void catchUp()
  {
    if (baseSeconds <= 0) {
      baseSeconds = (long)iterator.getBaseSeconds() << 32;
      logger.debug("Set the base seconds to {}", Codec.getStringWindowId(baseSeconds));
    }
    int intervalMillis;

    if (isReady()) {
      logger.debug("catching up {}->{}", upstream, group);
      try {
        /*
         * fast forward to catch up with the windowId without consuming
         */
        outer:
        while (ready && iterator.hasNext()) {
          SerializedData data = iterator.next();
          switch (data.bytes[data.dataOffset]) {
            case MessageType.RESET_WINDOW_VALUE:
              Tuple tuple = Tuple.getTuple(data.bytes, data.dataOffset, data.size - data.dataOffset + data.offset);
              baseSeconds = (long)tuple.getBaseSeconds() << 32;
              intervalMillis = tuple.getWindowWidth();
              if (intervalMillis <= 0) {
                logger.warn("Interval value set to non positive value = {}", intervalMillis);
              }
              ready = GiveAll.getInstance().distribute(physicalNodes, data);
              break;

            case MessageType.BEGIN_WINDOW_VALUE:
              tuple = Tuple.getTuple(data.bytes, data.dataOffset, data.size - data.dataOffset + data.offset);
              logger.debug("{}->{} condition {} =? {}",
                           new Object[] {
                upstream,
                group,
                Codec.getStringWindowId(baseSeconds | tuple.getWindowId()),
                Codec.getStringWindowId(skipWindowId)
              });
              if ((baseSeconds | tuple.getWindowId()) > skipWindowId) {
                logger.debug("caught up {}->{}", upstream, group);
                ready = GiveAll.getInstance().distribute(physicalNodes, data);
                caughtup = true;
                break outer;
              }
              break;

            case MessageType.CHECKPOINT_VALUE:
            case MessageType.CODEC_STATE_VALUE:
              ready = GiveAll.getInstance().distribute(physicalNodes, data);
              break;
          }
        }
      }
      catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }

      if (iterator.hasNext()) {
        addedData();
      }
    }
  }

  @SuppressWarnings("fallthrough")
  @Override
  public void addedData()
  {
    if (isReady()) {
      if (caughtup) {
        try {
          /*
           * consume as much data as you can before running out of steam
           */
          if (partitions.isEmpty()) {
            while (ready && iterator.hasNext()) {
              SerializedData data = iterator.next();
              switch (data.bytes[data.dataOffset]) {
                case MessageType.PAYLOAD_VALUE:
                  ready = policy.distribute(physicalNodes, data);
                  break;

                case MessageType.NO_MESSAGE_VALUE:
                case MessageType.NO_MESSAGE_ODD_VALUE:
                  break;

                case MessageType.RESET_WINDOW_VALUE:
                  Tuple resetWindow = Tuple.getTuple(data.bytes, data.dataOffset, data.size - data.dataOffset + data.offset);
                  baseSeconds = (long)resetWindow.getBaseSeconds() << 32;

                default:
                  //logger.debug("sending data of type {}", MessageType.valueOf(data.bytes[data.dataOffset]));
                  ready = GiveAll.getInstance().distribute(physicalNodes, data);
                  break;
              }
            }
          }
          else {
            while (ready && iterator.hasNext()) {
              SerializedData data = iterator.next();
              switch (data.bytes[data.dataOffset]) {
                case MessageType.PAYLOAD_VALUE:
                  Tuple tuple = Tuple.getTuple(data.bytes, data.dataOffset, data.size - data.dataOffset + data.offset);
                  int value = tuple.getPartition();
                  for (BitVector bv: partitions) {
                    if (bv.matches(value)) {
                      ready = policy.distribute(physicalNodes, data);
                      break;
                    }
                  }
                  break;

                case MessageType.NO_MESSAGE_VALUE:
                case MessageType.NO_MESSAGE_ODD_VALUE:
                  break;

                case MessageType.RESET_WINDOW_VALUE:
                  tuple = Tuple.getTuple(data.bytes, data.dataOffset, data.size - data.dataOffset + data.offset);
                  baseSeconds = (long)tuple.getBaseSeconds() << 32;

                default:
                  ready = GiveAll.getInstance().distribute(physicalNodes, data);
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
  }

  /**
   *
   * @param partitions
   * @return int
   */
  @Override
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

  public void boot(EventLoop eventloop)
  {
    for (PhysicalNode pn: physicalNodes) {
      eventloop.disconnect(pn.getClient());
      physicalNodes.clear();
    }
  }

  @Override
  public String toString()
  {
    return "LogicalNode{" + "upstream=" + upstream + ", group=" + group + ", partitions=" + partitions + '}';
  }

  private static final Logger logger = LoggerFactory.getLogger(LogicalNode.class);
}
