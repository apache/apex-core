/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.policy.GiveAll;
import com.malhartech.bufferserver.policy.Policy;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import org.jboss.netty.channel.Channel;

/**
 * LogicalNode represents a logical node in a DAG. Logical node can be split
 * into multiple physical nodes. The type of the logical node groups the
 * multiple physical nodes together in a group.
 *
 * @author chetan
 */
public class LogicalNode implements DataListener {

    private final String group;
    private final HashSet<PhysicalNode> physicalNodes;
    private final HashSet<ByteBuffer> partitions;
    private final Policy policy;
    private final Iterator<Data> iterator;
    private Object attachment;

    public LogicalNode(String group, Iterator<Data> iterator, Policy policy) {
        this.group = group;
        this.policy = policy;
        this.physicalNodes = new HashSet<PhysicalNode>();
        this.partitions = new HashSet<ByteBuffer>();
        this.iterator = iterator;
    }

    public String getGroup() {
        return group;
    }

    public Iterator<Data> getIterator() {
        return iterator;
    }

    public void addChannel(Channel channel) {
        PhysicalNode pn = new PhysicalNode(channel);
        if (!physicalNodes.contains(pn)) {
            physicalNodes.add(pn);
        }
    }

    public void addPartition(ByteBuffer partition) {
        partitions.add(partition);
    }

    /*
    public void injectData(Data data) {
        for (Channel pn : physicalNodes) {
            if (policy.confirms(this, data)) {
                node.write(data);
            }
        }
    }
    */

    public void catchUp(long windowId) {
        
        /* fast forward to catch up with the windowId without consuming */
        while (iterator.hasNext()) {
            Data data = iterator.next();
            if (data.getType() == Data.DataType.BEGIN_WINDOW) {
                if (data.getWindowId() >= windowId) {
                    policy.distribute(physicalNodes, data);
                }
            }
        }
        
        dataAdded(DataListener.NULL_PARTITION);
    }

    public void dataAdded(ByteBuffer partition) {
        /* consume as much data as you can before running out of steam */
        // my assumption is that one will never get blocked while writing
        // since the underlying blocking queue maintained by netty has infinite
        // capacity. we will have to double check though.
        while (iterator.hasNext()) {
            Data data = iterator.next();
            switch (data.getType()) {
                case PARTITIONED_DATA:
                    if (partitions.contains(data.getPartitioneddata().getPartition().asReadOnlyByteBuffer())) {
                        policy.distribute(physicalNodes, data);
                    }
                    break;

                case SIMPLE_DATA:
                    if (partitions.isEmpty()) {
                        policy.distribute(physicalNodes, data);
                    }
                    break;

                default:
                    GiveAll.getInstance().distribute(physicalNodes, data);
                    break;
            }
        }
    }

    public int getPartitions(Collection<ByteBuffer> partitions) {
        partitions.addAll(this.partitions);
        return partitions.size();
    }
}
