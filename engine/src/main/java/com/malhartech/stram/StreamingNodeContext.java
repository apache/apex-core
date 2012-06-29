/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.stram;

import java.util.Map;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 *
 * @author thomas
 */
public class StreamingNodeContext extends StreamingNodeUmbilicalProtocol.WritableAdapter
{

  private static final long serialVersionUID = 1L;
  private Map<String, String> properties;
  private String dnodeClassName;
  private String dnodeId;
  private String logicalId;
  /**
   * The window sequence initial value. Since nodes can be dynamically
   * allocated, they may start their processing at any window boundary.
   */
  public int startWindowSeq;
  /**
   * Window size. Start nodes in the DAG generate BEGIN_WINDOW at this interval.
   */
  public long windowSizeMillis;
  /**
   * Node should start processing the initial window at this time.
   */
  public long startWindowBeginMillis;

  // TODO: Further details
  // heartbeat interval    
  public Map<String, String> getProperties()
  {
    return properties;
  }

  public void setProperties(Map<String, String> properties)
  {
    this.properties = properties;
  }

  public String getDnodeClassName()
  {
    return dnodeClassName;
  }

  public void setDnodeClassName(String dnodeClassName)
  {
    this.dnodeClassName = dnodeClassName;
  }

  public String getDnodeId()
  {
    return dnodeId;
  }

  public void setDnodeId(String dnodeId)
  {
    this.dnodeId = dnodeId;
  }

  public String getLogicalId()
  {
    return logicalId;
  }

  public void setLogicalId(String logicalId)
  {
    this.logicalId = logicalId;
  }

  public int getStartWindowSeq()
  {
    return startWindowSeq;
  }

  public void setStartWindowSeq(int startWindowSeq)
  {
    this.startWindowSeq = startWindowSeq;
  }

  public long getWindowSizeMillis()
  {
    return windowSizeMillis;
  }

  public void setWindowSizeMillis(long windowSizeMillis)
  {
    this.windowSizeMillis = windowSizeMillis;
  }

  public long getStartWindowBeginMillis()
  {
    return startWindowBeginMillis;
  }

  public void setStartWindowBeginMillis(long startWindowBeginMillis)
  {
    this.startWindowBeginMillis = startWindowBeginMillis;
  }

  @Override
  public String toString()
  {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("id", this.dnodeId).
            append("logicalId", this.logicalId).
            append("dnodeClassName", this.dnodeClassName).
            toString();
  }
}
