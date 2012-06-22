/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech;

import com.malhartech.Buffer.Data;
import com.malhartech.policy.Policy;
import com.malhartech.policy.PolicyContext;
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
public class LogicalNode implements PolicyContext, DataListener {

    private final String group;
    private final HashSet<Channel> physicalNodes;
    private final HashSet<ByteBuffer> partitions;
    private final Policy policy;
    private final Iterator<Data> iterator;
    Object attachment;

    public LogicalNode(String group, Iterator<Data> iterator, Policy policy) {
        this.group = group;
        this.policy = policy;
        this.physicalNodes = new HashSet<Channel>();
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
        physicalNodes.add(channel);
    }

    public void addPartition(ByteBuffer partition) {
        partitions.add(partition);
    }

    public void injectData(Data data) {
        for (Channel node : physicalNodes) {
            if (policy.confirms(this, data)) {
                node.write(data);
            }
        }
    }

    public void setAttachment(Object o) {
        attachment = o;
    }

    public Object getAttachment() {
        return attachment;
    }

    public Channel getChannel() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void catchUp(long windowId) {
        
        /* fast forward to catch up with the windowId without consuming */
        while (iterator.hasNext()) {
            Data data = iterator.next();
            if (data.getType() == Data.DataType.BEGIN_WINDOW) {
                if (data.getBeginwindow().getWindowId() >= windowId) {
                    for (Channel node : physicalNodes) {
                        node.write(data);
                        break;
                    }
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
                        for (Channel node : physicalNodes) {
                            if (policy.confirms(this, data)) {
                                node.write(data);
                            }
                        }
                    }
                    break;

                case SIMPLE_DATA:
                    if (partitions.isEmpty()) {
                        for (Channel node : physicalNodes) {
                            if (policy.confirms(this, data)) {
                                node.write(data);
                            }
                        }
                    }
                    break;

                default:
                    for (Channel node : physicalNodes) {
                        node.write(data);
                    }
                    break;
            }
        }
    }

    public int getPartitions(Collection<ByteBuffer> partitions) {
        partitions.addAll(this.partitions);
        return partitions.size();
    }
}
