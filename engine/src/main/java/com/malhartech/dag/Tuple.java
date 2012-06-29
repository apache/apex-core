/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.dag;

import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.stram.StreamContext;

/**
 *
 * @author chetan
 */
public class Tuple {
    StreamContext ctx;
    Data data;
    Object object;

    Tuple(Object object, StreamContext stream) {
        this.object = object;
        this.ctx = stream;
    }
    
    public void setData(Data data) {
      this.data = data;
    }
    
    public Data getData()
    {
      return data;
    }
}
