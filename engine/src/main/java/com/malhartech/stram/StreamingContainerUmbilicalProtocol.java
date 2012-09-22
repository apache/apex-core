/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.VersionedProtocol;

import com.malhartech.dag.HeartbeatCounters;

/**
 * Protocol that streaming node child process uses to contact its parent
 * (application master) process
 * <p>
 * <br>
 * All communication between child and parent is via this protocol.
 *
 * <br>
 */
// @TokenInfo(JobTokenSelector.class)
@InterfaceAudience.Private
@InterfaceStability.Stable
public interface StreamingContainerUmbilicalProtocol extends VersionedProtocol {
  public static final long versionID = 201208081755L;

  void log(String containerId, String msg) throws IOException;

  /**
   * TODO: quick hack to focus on protocol instead of serialization code -
   * replace with PB
   */
  public static abstract class WritableAdapter implements Writable, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public void readFields(DataInput arg0) throws IOException {
      int len = arg0.readInt();
      byte[] bytes = new byte[len];
      arg0.readFully(bytes);
      try {
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) ois.readObject();
        Field[] fields = this.getClass().getDeclaredFields();
        AccessibleObject.setAccessible(fields, true);
        for (int i = 0; i < fields.length; i++) {
          Field field = fields[i];
          String fieldName = field.getName();
          if (properties.containsKey(fieldName)) {
            field.set(this, properties.get(fieldName));
          }
        }
        ois.close();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(bos);
      try {
        Map<String, Object> properties = new java.util.HashMap<String, Object>();
        Field[] fields = this.getClass().getDeclaredFields();
        AccessibleObject.setAccessible(fields, true);
        for (int i = 0; i < fields.length; i++) {
          Field field = fields[i];
          if (!Modifier.isStatic(field.getModifiers())) {
            String fieldName = field.getName();
            Object fieldValue = field.get(this);
            properties.put(fieldName, fieldValue);
          }
        }
        oos.writeObject(properties);
      } catch (Exception e) {
        throw new IOException(e);
      }
      oos.flush();
      byte[] bytes = bos.toByteArray();
      arg0.writeInt(bytes.length);
      arg0.write(bytes);
      oos.close();
    }
  }

  /**
   * Initialization parameters for StramChild container container. Container
   * wide settings remain effective as long as the process is running. Nodes can
   * be deployed and removed dynamically.
   * <p>
   * <br>
   *
   */
  public static class StreamingContainerContext extends WritableAdapter {
    private static final long serialVersionUID = 201209071402L;

    /**
     * The list of operators to initially deploy in the container.
     */
    public List<ModuleDeployInfo> nodeList;

    /**
     * How frequently should operators heartbeat to stram. Recommended setting is
     * 1000ms. Can be set to 0 for unit testing.
     */
    private long heartbeatIntervalMillis;

    public long getHeartbeatIntervalMillis() {
      return heartbeatIntervalMillis;
    }

    public void setHeartbeatIntervalMillis(long heartbeatIntervalMillis) {
      this.heartbeatIntervalMillis = heartbeatIntervalMillis;
    }

    /**
     * Window size. Inputs into the DAG propagate BEGIN_WINDOW at this interval.
     */
    private int windowSizeMillis;
    /**
     * Node should start processing the initial window at this time.
     */
    private long startWindowMillis;

    public int getWindowSizeMillis() {
      return windowSizeMillis;
    }

    public void setWindowSizeMillis(int windowSizeMillis) {
      this.windowSizeMillis = windowSizeMillis;
    }

    public long getStartWindowMillis() {
      return startWindowMillis;
    }

    public void setStartWindowMillis(long startWindowBeginMillis) {
      this.startWindowMillis = startWindowBeginMillis;
    }

    private String checkpointDfsPath;

    public String getCheckpointDfsPath() {
      return checkpointDfsPath;
    }

    public void setCheckpointDfsPath(String dfsPath) {
      this.checkpointDfsPath = dfsPath;
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
          .append("operators", this.nodeList).toString();
    }
  }

  /**
   *
   * The child obtains its configuration context after container launch.
   * <p>
   * <br>
   * Context will provide all information to initialize or reconfigure the
   * node(s)<br>
   *
   * @param containerId
   * @throws IOException
   * <br>
   */
  StreamingContainerContext getInitContext(String containerId) throws IOException;

  /**
   *
   * Stats of the node that is sent to the hadoop container
   * <p>
   * <br>
   * Hadoop container wraps this together with stats from other operators and sends
   * it to stram <br>
   */
  public static class StreamingNodeHeartbeat extends WritableAdapter {
    private static final long serialVersionUID = 201208171625L;
    private ArrayList<HeartbeatCounters> heartbeatsContainer = new ArrayList<HeartbeatCounters>();

    /**
     * @return the heartbeatsCointainer
     */
    public ArrayList<HeartbeatCounters> getHeartbeatsContainer() {
      return heartbeatsContainer;
    }

    /**
     * @param heartbeatsCointainer
     *          the heartbeatsCointainer to set
     */
    public void setHeartbeatsContainer(ArrayList<HeartbeatCounters> heartbeatsCointainer) {
      this.heartbeatsContainer = heartbeatsCointainer;
    }

    public static enum DNodeState {
      NEW, // node instantiated but not processing yet
      PROCESSING, IDLE // the node stopped processing (no more input etc.)
    }

    /**
     * The originating node. There can be multiple operators in a container.
     */
    private String nodeId;

    public String getNodeId() {
      return nodeId;
    }

    public void setNodeId(String nodeId) {
      this.nodeId = nodeId;
    }

    /**
     * Time when the heartbeat was generated by the node.
     */
    private long generatedTms;

    public long getGeneratedTms() {
      return generatedTms;
    }

    public void setGeneratedTms(long generatedTms) {
      this.generatedTms = generatedTms;
    }

    /**
     * Number of milliseconds elapsed since last heartbeat. Other statistics
     * relative to this interval.
     */
    private long intervalMs;

    public long getIntervalMs() {
      return intervalMs;
    }

    public void setIntervalMs(long intervalMs) {
      this.intervalMs = intervalMs;
    }

    /**
     * State of the dnode (processing, idle etc).
     */
    private String state;

    public String getState() {
      return state;
    }

    public void setState(String state) {
      this.state = state;
    }

    /**
     * Number of tuples processed within the heartbeat interval. Stram can use
     * this as bottleneck indicator and ask the node for partitioning
     * information/option to load balance.
     */
    public int getNumberTuplesProcessed() {
      int numberTuplesProcessed = 0;

      for (HeartbeatCounters hc : heartbeatsContainer) {
        numberTuplesProcessed += hc.tuplesProcessed;
      }

      return numberTuplesProcessed;
    }

    /**
     * Number of bytes processed during the heartbeat interval.
     */
    public int getNumberBytesProcessed() {
      int numberBytesProcessed = 0;

      for (HeartbeatCounters hc : heartbeatsContainer) {
        numberBytesProcessed += hc.bytesProcessed;
      }

      return numberBytesProcessed;
    }

    private long lastBackupWindowId;

    public long getLastBackupWindowId() {
      return lastBackupWindowId;
    }

    public void setLastBackupWindowId(long lastBackupWindowId) {
      this.lastBackupWindowId = lastBackupWindowId;
    }
  }

  /**
   *
   * Sends stats aggregated by all operators in the this container to the stram
   * <p>
   * <br>
   *
   */
  public static class ContainerHeartbeat extends WritableAdapter {
    private static final long serialVersionUID = 1L;
    private String containerId;

    public String getContainerId() {
      return containerId;
    }

    public void setContainerId(String containerId) {
      this.containerId = containerId;
    }

    /**
     * List with all operators in the container.
     */
    private List<StreamingNodeHeartbeat> dnodeEntries;

    public List<StreamingNodeHeartbeat> getDnodeEntries() {
      return dnodeEntries;
    }

    public void setDnodeEntries(List<StreamingNodeHeartbeat> dnodeEntries) {
      this.dnodeEntries = dnodeEntries;
    }
  }

  /**
   *
   * Request by stram as response to heartbeat for further communication
   * <p>
   * <br>
   * The child container will continue RPC communication depending on the type
   * of request.<br>
   * <br>
   *
   */
  public static class StramToNodeRequest extends WritableAdapter {
    private static final long serialVersionUID = 1L;

    enum RequestType {
      REPORT_PARTION_STATS, CHECKPOINT
    }

    private String nodeId;
    private RequestType requestType;

    public String getNodeId() {
      return nodeId;
    }

    public void setNodeId(String nodeId) {
      this.nodeId = nodeId;
    }

    public RequestType getRequestType() {
      return requestType;
    }

    public void setRequestType(RequestType requestType) {
      this.requestType = requestType;
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
          .append("nodeId", this.nodeId)
          .append("requestType", this.requestType).toString();
    }
  }

  /**
   *
   * Response from the stram to the container heartbeat
   * <p>
   * <br>
   *
   */
  public static class ContainerHeartbeatResponse extends WritableAdapter {
    private static final long serialVersionUID = 1L;
    /**
     * Indicate container to exit heartbeat loop and shutdown.
     */
    public boolean shutdown;

    /**
     * Optional list of responses for operators in the container.
     */
    public List<StramToNodeRequest> nodeRequests;

    /**
     * Set when there are pending requests that wait for dependencies to
     * complete
     */
    public boolean hasPendingRequests = false;

    /**
     * Set when operators need to be removed
     */
    public List<ModuleDeployInfo> undeployRequest;

    /**
     * Set when new operators need to be deployed
     */
    public List<ModuleDeployInfo> deployRequest;

  }

  /**
   * To be called periodically by child for heartbeat protocol. Container may
   * return response for node to shutdown etc.
   */
  ContainerHeartbeatResponse processHeartbeat(ContainerHeartbeat msg);

  /**
   * Called to fetch pending request.
   *
   * @param containerId
   * @return {com.malhartech.stram.ContainerHeartbeatResponse}
   */
  ContainerHeartbeatResponse pollRequest(String containerId);

  /**
   * Reporting of partitioning stats - requested by stram for operators that
   * participate in partitioning when the basic heartbeat indicates a
   * bottleneck. The details would then be used by stram to split or merge operators
   * to re-balance load.
   *
   * @return {com.malhartech.stram.StramToNodeRequest}
   */
  StramToNodeRequest processPartioningDetails();
}
