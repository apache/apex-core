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
public interface Policy {
    public boolean confirms(PolicyContext pc, Data d);
}
