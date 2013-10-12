/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.webapp;


import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import com.datatorrent.stram.engine.Stats;

/**
 * <p>PortInfo class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.2
 */
@XmlRootElement(name = "port")
@XmlAccessorType(XmlAccessType.FIELD)
public class PortInfo
{
  public String name;
  public String type;
  public long totalTuples;
  public long tuplesPSMA;
  public long bufferServerBytesPSMA;
  public long recordingStartTime = Stats.INVALID_TIME_MILLIS;
}
