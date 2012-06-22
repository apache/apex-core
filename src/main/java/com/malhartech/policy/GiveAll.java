/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.policy;

import com.malhartech.Buffer.Data;

/**
 *
 * @author chetan
 */
public class GiveAll extends AbstractPolicy {
    
    final static Policy giveall = new GiveAll();
    
    private GiveAll()
    {
        
    }

    @Override
    public boolean confirms(PolicyContext pc, Data d) {
        return true;
    }
    
    public static Policy getInstance()
    {
        return giveall;
    }
    
}
