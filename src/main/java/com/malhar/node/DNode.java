package com.malhar.node;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * Placeholder for topology builder test
 */
public abstract class DNode implements Configurable {
  protected Configuration conf;
  
  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
}
