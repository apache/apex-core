/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.internal;

import com.malhartech.bufferserver.client.VarIntLengthPrependerClient;
import com.malhartech.bufferserver.packet.MessageType;
import com.malhartech.bufferserver.packet.Tuple;
import com.malhartech.bufferserver.policy.GiveAll;
import com.malhartech.bufferserver.policy.Policy;
import com.malhartech.bufferserver.util.BitVector;
import com.malhartech.bufferserver.util.Codec;
import com.malhartech.bufferserver.util.SerializedData;
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
  private final Policy policy = GiveAll.getInstance();
  private final DataListIterator iterator;
  private final long windowId;
  private long baseSeconds;
  private boolean caughtup;

  /**
   *
   * @param upstream
   * @param group
   * @param iterator
   * @param startingWindowId
   */
  public LogicalNode(String upstream, String group, Iterator<SerializedData> iterator, long startingWindowId)
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
   * @param connection
   */
  public void addConnection(VarIntLengthPrependerClient connection)
  {
    PhysicalNode pn = new PhysicalNode(connection);
    if (!physicalNodes.contains(pn)) {
      physicalNodes.add(pn);
    }
  }

  /**
   *
   * @param connection
   */
  public void removeChannel(VarIntLengthPrependerClient connection)
  {
    for (PhysicalNode pn : physicalNodes) {
      if (pn.getConnection() == connection) {
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

  public boolean isReady()
  {
//    if (blocked) {
//      for (PhysicalNode pn: physicalNodes) {
//        if (pn.isBlocked()) {
//          return false;
//        }
//      }
//      blocked = false;
//    }

    return true;
  }

  // make it run a lot faster by tracking faster!
  /**
   *
   */
  public void catchUp()
  {
    int intervalMillis;

    if (isReady()) {
//    logger.debug("catching up {}->{}", upstream, group);
      try {
        /*
         * fast forward to catch up with the windowId without consuming
         */
        outer:
        while (iterator.hasNext()) {
          SerializedData data = iterator.next();
          switch (data.bytes[data.dataOffset]) {
            case MessageType.RESET_WINDOW_VALUE:
              Tuple tuple = Tuple.getTuple(data.bytes, data.dataOffset, data.size - data.dataOffset + data.offset);
              baseSeconds = (long)tuple.getBaseSeconds() << 32;
              intervalMillis = tuple.getWindowWidth();
              if (intervalMillis <= 0) {
                logger.warn("Interval value set to non positive value = {}", intervalMillis);
              }
              GiveAll.getInstance().distribute(physicalNodes, data);
              break;

            case MessageType.BEGIN_WINDOW_VALUE:
              tuple = Tuple.getTuple(data.bytes, data.dataOffset, data.size - data.dataOffset + data.offset);
              logger.debug("{}->{} condition {} =? {}",
                           new Object[] {
                upstream,
                group,
                Codec.getStringWindowId(baseSeconds | tuple.getWindowId()),
                Codec.getStringWindowId(windowId)
              });
              if ((baseSeconds | tuple.getWindowId()) >= windowId) {
                logger.debug("caught up {}->{}", upstream, group);
                GiveAll.getInstance().distribute(physicalNodes, data);
                caughtup = true;
                break outer;
              }
              break;

            case MessageType.CHECKPOINT_VALUE:
            case MessageType.CODEC_STATE_VALUE:
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
  }

  @SuppressWarnings("fallthrough")
  @Override
  public void dataAdded()
  {
    if (isReady()) {
      if (caughtup) {
        try {
          /*
           * consume as much data as you can before running out of steam
           */
          if (partitions.isEmpty()) {
            while (iterator.hasNext()) {
              SerializedData data = iterator.next();
              switch (data.bytes[data.dataOffset]) {
                case MessageType.PAYLOAD_VALUE:
                  policy.distribute(physicalNodes, data);
                  break;

                case MessageType.NO_MESSAGE_VALUE:
                case MessageType.NO_MESSAGE_ODD_VALUE:
                  break;

                case MessageType.RESET_WINDOW_VALUE:
                  Tuple resetWindow = Tuple.getTuple(data.bytes, data.dataOffset, data.size - data.dataOffset + data.offset);
                  baseSeconds = (long)resetWindow.getBaseSeconds() << 32;

                default:
                  GiveAll.getInstance().distribute(physicalNodes, data);
                  break;
              }
            }
          }
          else {
            while (iterator.hasNext()) {
              SerializedData data = iterator.next();
              switch (data.bytes[data.dataOffset]) {
                case MessageType.PAYLOAD_VALUE:
                  Tuple tuple = Tuple.getTuple(data.bytes, data.dataOffset, data.size - data.dataOffset + data.offset);
                  int value = tuple.getPartition();
                  for (BitVector bv : partitions) {
                    if (bv.matches(value)) {
                      policy.distribute(physicalNodes, data);
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

}
