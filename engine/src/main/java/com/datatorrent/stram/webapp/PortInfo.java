/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.webapp;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
@XmlRootElement(name = "port")
@XmlAccessorType(XmlAccessType.FIELD)
public class PortInfo
{
  public String name;
  public long totalTuples;
  public long tuplesPSMA10;
  public long bufferServerBytesPSMA10;  // TBD
}
