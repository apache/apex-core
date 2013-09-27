/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;


import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Component;
import com.datatorrent.api.ComponentComplementPair;
import com.datatorrent.api.Context;


/**
 * <p>ComponentConfigurationPair class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public class ComponentConfigurationPair<COMPONENT extends Component<Context>, CONFIG extends Configuration> extends ComponentComplementPair<COMPONENT, CONFIG>
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
