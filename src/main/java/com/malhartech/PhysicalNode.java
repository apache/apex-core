/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech;

import com.malhartech.Buffer.Data;
import org.jboss.netty.channel.Channel;

/**
 *
 * @author chetan
 */
public class PhysicalNode {
    private final long starttime;
    private final Channel channel;
    private long processedMessageCount;
    
    
    public PhysicalNode(Channel channel) {
        this.channel = channel;
        starttime = System.currentTimeMillis();
        processedMessageCount = 0;
    }
    
    public long getstartTime()
    {
        return starttime;
    }
    
    public long getUptime()
    {
        return System.currentTimeMillis() - starttime;
    }
    
    public void send(Data d) {
        channel.write(d);
        processedMessageCount++;
    }
    
    public long getProcessedMessageCount() {
        return processedMessageCount;
    }
    
    @Override
    public boolean equals(Object o) {
        return o.hashCode() == this.hashCode();
    }
    
    public int getId()
    {
        return channel.getId();
    }
    
    @Override
    public int hashCode()
    {
        return channel.getId();
    }
}
