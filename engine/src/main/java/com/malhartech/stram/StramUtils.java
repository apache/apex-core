/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.util.Map;

import com.malhartech.dag.DefaultSerDe;
import com.malhartech.dag.SerDe;
import com.malhartech.stram.conf.TopologyBuilder;

/**
 * Utilities for shared use in Stram components.
 */
public abstract class StramUtils {

  public static SerDe getSerdeInstance(Map<String, String> streamProps) {
    String className = streamProps.get(TopologyBuilder.STREAM_SERDE_CLASSNAME);
    if (className != null) {
      try {
        Class<? extends SerDe> serdeClass = Class.forName(className).asSubclass(SerDe.class);
        return serdeClass.newInstance();
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("SerDe class not found: " + className, e);
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException("Failed to instantiate SerDe", e);
      } catch (InstantiationException e) {
        throw new IllegalArgumentException("Failed to instantiate SerDe", e);
      }      
    } else {
      return new DefaultSerDe();
    }
  }
}
