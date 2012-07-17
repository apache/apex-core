/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.dag;

/**
 *
 * @author chetan
 */
public interface SerDe
{
  Object fromByteArray(byte[] bytes);
  byte[] toByteArray(Object o);
  byte[] getPartition(Object o);

  /**
   * Possible partitions that can be generated.
   * Currently stram assumes that this is idempotent.
   * @return
   */
  byte[][] getPartitions();
  
}
