/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.webapp;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
@XmlRootElement(name = "stream")
@XmlAccessorType(XmlAccessType.FIELD)
public class StreamInfo
{
  public static class Port {
    public String operatorId;
    public String portName;
  }
  public String logicalName;
  public Port source = new Port();
  public List<Port> sinks = new ArrayList<Port>();
}
