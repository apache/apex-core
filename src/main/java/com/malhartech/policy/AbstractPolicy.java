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
public class AbstractPolicy implements Policy {

    public boolean confirms(PolicyContext pc, Data d) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    public static Policy getInstance()
    {
        return null;
    }
    
}
