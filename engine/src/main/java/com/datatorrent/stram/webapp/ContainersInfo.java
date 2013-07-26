/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.webapp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * Provides plan level operator data<p>
 * <br>
 * This call provides restful access to individual operator instance data<br>
 * <br>
 *
 * @since 0.3.2
 */
@XmlRootElement(name = "containers")
@XmlAccessorType(XmlAccessType.FIELD)
public class ContainersInfo
{
  protected ArrayList<ContainerInfo> containers = new ArrayList<ContainerInfo>();

  /**
   *
   * @param info
   */
  public void add(ContainerInfo info)
  {
    containers.add(info);
  }

  /**
   *
   * @return ArrayList<ContainerInfo>
   *
   */
  public Collection<ContainerInfo> getContainers()
  {
    return Collections.unmodifiableCollection(containers);
  }

}
