/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.stram.engine;

import com.datatorrent.api.StringCodec;
import java.io.Serializable;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class GenericOperatorProperty
{
  private String s;

  public GenericOperatorProperty(String s)
  {
    this.s = s;
  }

  // This is on purpose not to be a getter.
  public String obtainString()
  {
    return this.s;
  }

  public static class GenericOperatorPropertyStringCodec implements StringCodec<GenericOperatorProperty>, Serializable
  {
    private static final long serialVersionUID = 201403031223L;

    @Override
    public GenericOperatorProperty fromString(String string)
    {
      return new GenericOperatorProperty(string);
    }

    @Override
    public String toString(GenericOperatorProperty pojo)
    {
      return pojo.obtainString();
    }

  }
}
