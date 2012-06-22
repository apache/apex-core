/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech;

import java.nio.ByteBuffer;
import java.util.Collection;

/**
 *
 * @author chetan
 */
public interface DataListener {

    public static final ByteBuffer NULL_PARTITION = ByteBuffer.allocate(0);

    public void dataAdded(ByteBuffer partition);

    public int getPartitions(Collection<ByteBuffer> partitions);
}
