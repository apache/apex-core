/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.dag;

import com.malhartech.bufferserver.Buffer.Data;

/**
 *
 * @author chetan
 */
public class Tuple {
    Object stream;
    Data data;
    Object object;

    Tuple(Object object, Object stream) {
        this.object = object;
        this.stream = stream;
    }
    
    public void setData(Data data) {
      this.data = data;
    }
    
    public Data getData()
    {
      return data;
    }
}
