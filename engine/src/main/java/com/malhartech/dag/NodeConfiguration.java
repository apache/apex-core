/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.malhartech.dag;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class NodeConfiguration extends Configuration 
{
  private final Map<String, String> properties;
  
  public NodeConfiguration(Map<String, String> properties) {
    this.properties = properties;
    addAll(this, properties);
  }

  public Map<String, String> getDagProperties() {
    return properties;
  }
  
  public static void addAll(Configuration conf, Map<String, String> properties) {
    for (Map.Entry<String, String> e : properties.entrySet()) {
      conf.set(e.getKey(), e.getValue());
    }    
  }
  
}
