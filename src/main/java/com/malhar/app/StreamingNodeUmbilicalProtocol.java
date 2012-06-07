package com.malhar.app;

import java.io.IOException;

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
  
  void echo(String msg) throws IOException;
  
}
