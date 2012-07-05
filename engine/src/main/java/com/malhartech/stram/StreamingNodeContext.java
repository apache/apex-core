/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.stram;

import com.malhartech.dag.Context;
import java.util.Map;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 *
 * @author thomas
 */
public class StreamingNodeContext extends StreamingNodeUmbilicalProtocol.WritableAdapter implements Context
{

  private static final long serialVersionUID = 1L;
  private Map<String, String> properties;
  private String dnodeClassName;
  private String dnodeId;
  private String logicalId;

  /**
   * Window size. Inputs in the DAG generate BEGIN_WINDOW at this interval.
   */
  public long windowSizeMillis;
  /**
   * Node should start processing the initial window at this time.
   */
  public long startWindowMillis;


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

  public long getWindowSizeMillis()
  {
    return windowSizeMillis;
  }

  public void setWindowSizeMillis(long windowSizeMillis)
  {
    this.windowSizeMillis = windowSizeMillis;
  }

  public long getStartWindowMillis()
  {
    return startWindowMillis;
  }

  public void setStartWindowMillis(long startWindowBeginMillis)
  {
    this.startWindowMillis = startWindowBeginMillis;
  }

  @Override
  public String toString()
  {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("id", this.dnodeId).
            append("logicalId", this.logicalId).
            append("dnodeClassName", this.dnodeClassName).
            toString();
  }

  public Object getAttachment()
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
