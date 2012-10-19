/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.malhartech.dag.Component;
import org.apache.hadoop.conf.Configuration;


/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ComponentConfigurationPair<COMPONENT extends Component, CONFIG extends Configuration> extends ComponentComplementPair<COMPONENT, CONFIG>
{
  public final CONFIG config;

  public ComponentConfigurationPair(COMPONENT component, CONFIG context)
  {
    super(component);
    this.config = context;
  }

  @Override
  public CONFIG getComplement()
  {
    return config;
  }
}
