/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.webapp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;

/**
 *
 * Provides plan level operator data<p>
 * <br>
 * This call provides restful access to individual operator instance data<br>
 * <br>
 *
 * @since 0.3.2
 */

@XmlRootElement(name = "streams")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlSeeAlso({StreamInfo.class})
public class StreamsInfo {

  protected List<StreamInfo> streams = new ArrayList<StreamInfo>();

  /**
   *
   * @param streamInfo
   */
  public void add(StreamInfo streamInfo) {
    streams.add(streamInfo);
  }

  /**
   *
   * @return list of stream info
   */
  public List<StreamInfo> getStreams() {
    return Collections.unmodifiableList(streams);
  }

}
