/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;


/**
 *
 * TBD<p>
 * <br>
 *
 * @author chetan
 */
public interface Operator extends Component<ModuleConfiguration, ModuleContext>
{
  /**
   * This method gets called at the beginning of each window.
   *
   */
  public void beginWindow();

  /**
   * This method gets called at the end of each window.
   *
   */
  public void endWindow();

}
