/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
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
 * TBD (Stram side implementation of Physcial Node?)<p>
 * <br>
 * @author thomas
 */

public class NodePConf extends StreamingNodeUmbilicalProtocol.WritableAdapter implements Context
{

  private static final long serialVersionUID = 1L;
  private Map<String, String> properties;
  private String dnodeClassName;
  private String dnodeId;
  private String logicalId;

  /**
   * 
   * @return Map<String, String>
   */
  public Map<String, String> getProperties()
  {
    return properties;
  }

  /**
   * 
   * @param properties 
   */
  public void setProperties(Map<String, String> properties)
  {
    this.properties = properties;
  }

  /**
   * 
   * @return String
   */
  public String getDnodeClassName()
  {
    return dnodeClassName;
  }

  /**
   * 
   * @param dnodeClassName 
   */
  public void setDnodeClassName(String dnodeClassName)
  {
    this.dnodeClassName = dnodeClassName;
  }

  /**
   * 
   * @return String
   */
  public String getDnodeId()
  {
    return dnodeId;
  }

  /**
   * 
   * @param dnodeId 
   */
  public void setDnodeId(String dnodeId)
  {
    this.dnodeId = dnodeId;
  }

  /**
   * 
   * @return String
   */
  public String getLogicalId()
  {
    return logicalId;
  }

  /**
   * 
   * @param logicalId 
   */
  public void setLogicalId(String logicalId)
  {
    this.logicalId = logicalId;
  }

  private long checkpointWindowId;
  
  /**
   * The checkpoint window identifier.
   * Used to restore node and incoming streams as part of recovery.
   * Value 0 indicates fresh initialization, no restart.   
   * @return long
   */
  public long getCheckpointWindowId() {
    return checkpointWindowId;
  }

  /**
   * 
   * @param checkpointWindowId 
   */
  public void setCheckpointWindowId(long checkpointWindowId) {
    this.checkpointWindowId = checkpointWindowId;
  }

  /**
   * 
   * @return String
   */
  @Override
  public String toString()
  {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("id", this.dnodeId).
            append("logicalId", this.logicalId).
            append("dnodeClassName", this.dnodeClassName).
            toString();
  }

}
