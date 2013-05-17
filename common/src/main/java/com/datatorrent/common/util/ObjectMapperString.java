/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 * For JSON raw serialization, assumes the passed string to be a valid javascript value representation
 */
public class ObjectMapperString
{
  public String string;

  public ObjectMapperString(String string)
  {
    this.string = string;
  }

  @Override
  public String toString()
  {
    return string;
  }

}