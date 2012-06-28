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
public class RoundRobin extends AbstractPolicy {
    
    int index;
    
    public RoundRobin() {
        index = 0;
    }
            

    @Override
    public void distribute(Set<PhysicalNode> nodes, Data data) {
        index %= nodes.size();
        int count = index++;
        for (PhysicalNode node : nodes) {
            if (count-- == 0) {
                node.send(data);
                break;
            }
        }
    }
    
}
