package com.malhar.app;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.VersionedProtocol;

/** Protocol that streaming node child process uses to contact its parent (application master) process. All communication between child
 * and parent is via this protocol. 
*/ 
//@TokenInfo(JobTokenSelector.class)
@InterfaceAudience.Private
@InterfaceStability.Stable
public interface StreamingNodeUmbilicalProtocol extends VersionedProtocol {

  public static final long versionID = 1L;
  
  void echo(String containerId, String msg) throws IOException;

  public static class StreamingNodeContext implements Serializable {
    private static final long serialVersionUID = 1L;
    public Map<String, String> properties;
    public String dnodeClassName;
    public String dnodeId;
    public String logicalId;

    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
          append("id", this.dnodeId).
          append("logicalId", this.logicalId).
          append("dnodeClassName", this.dnodeClassName).
          toString();
    }  
  
  }
  
  /**
   * The child obtains its context after container launch. Context will provide all information to initialize the node(s) 
   * @return
   * @throws IOException
   */
  StreamingNodeContext getNodeContext(String containerId) throws IOException;
  
}
