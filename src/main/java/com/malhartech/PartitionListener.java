/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech;

import java.nio.ByteBuffer;

/**
 *
 * @author chetan
 */
public interface PartitionListener {
    public static final ByteBuffer NULL_PARTITION = ByteBuffer.allocate(0);

    void elementAdded(ByteBuffer partition, DataList dl);
}
