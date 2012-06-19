/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech;

import com.malhartech.Buffer.Data;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import org.jboss.netty.channel.Channel;

/**
 *
 * @author chetan
 */
public class PartitionDataListener implements PartitionListener {

    private final Channel channel;
    private final Iterator iterator;
    private final HashSet<ByteBuffer> partitions;
    private final DataList datalist;

    public PartitionDataListener(Channel channel, DataList datalist, Iterator iterator) {
        this.channel = channel;
        this.iterator = iterator;
        this.datalist = datalist;
        partitions = new HashSet<ByteBuffer>();
    }
    
    public void addPartition(ByteBuffer bb)
    {
        partitions.add(bb);
    }
    
    public DataList getDataList()
    {
        return this.datalist;
    }
    
    public int drainPartitions(Collection<? super ByteBuffer> container)
    {
        if (container.addAll(partitions)) {
            return partitions.size();
        }
        
        return 0;
    }

    public void elementAdded(ByteBuffer partition, DataList dl) {
        while (iterator.hasNext()) {
            Data d = (Data) iterator.next();
            switch (d.getType()) {
                case PARTITIONED_DATA:
                    if (partitions.contains(d.getPartitioneddata().getPartition().asReadOnlyByteBuffer())) {
                        this.channel.write(d);
                    }

                    break;

                case SIMPLE_DATA:
                    if (partitions.contains(PartitionListener.NULL_PARTITION)) {
                        this.channel.write(d);
                    }
                    break;

                case BEGIN_WINDOW:
                case END_WINDOW:
                case HEARTBEAT_DATA:
                case SERDE_CODE:
                    this.channel.write(d);
                    break;
            }
        }
    }
}
