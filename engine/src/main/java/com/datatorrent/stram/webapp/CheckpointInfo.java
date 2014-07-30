package com.datatorrent.stram.webapp;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Created by gaurav on 7/22/14.
 *
 * @since 1.0.3
 */

@XmlRootElement(name = "checkpoint")
@XmlAccessorType(XmlAccessType.FIELD)
public class CheckpointInfo
{
  public long checkpointSize;
  public long checkpointTime;
}
