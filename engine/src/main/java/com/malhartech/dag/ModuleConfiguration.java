/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * Extends {@link org.apache.hadoop.conf.Configuration} for operators of the dag<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ModuleConfiguration extends Configuration
{
  private final Map<String, String> properties;

  public ModuleConfiguration(String id, Map<String, String> properties)
  {
    set("Id", id);
    this.properties = properties;
    addAll(this, properties);
  }

  public Map<String, String> getDagProperties()
  {
    return properties;
  }

  public static void addAll(Configuration conf, Map<String, String> properties)
  {
    if (properties != null) {
      for (Map.Entry<String, String> e: properties.entrySet()) {
        conf.set(e.getKey(), e.getValue());
      }
    }
  }
}
