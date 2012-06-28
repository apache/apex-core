/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.bufferserver.policy;

import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.PhysicalNode;
import java.util.Set;

/**
 *
 * @author chetan
 */
public class AbstractPolicy implements Policy {

    public void distribute(Set<PhysicalNode> nodes, Data data) {
        throw new UnsupportedOperationException("Not supported yet.");
    }    
}
