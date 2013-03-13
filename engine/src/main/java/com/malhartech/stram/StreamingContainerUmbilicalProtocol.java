/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.malhartech.api.DAGContext;
import com.malhartech.engine.OperatorStats;
import com.malhartech.util.AttributeMap;

import java.io.*;
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
   * Initialization parameters for StramChild container. Container
   * wide settings remain effective as long as the process is running. Operators can
   * be deployed and removed dynamically.
   * <p>
   * <br>
   *
   */
  public static class StreamingContainerContext extends WritableAdapter {
    private static final long serialVersionUID = 201209071402L;

    public AttributeMap<DAGContext> applicationAttributes;

    /**
     * Operators should start processing the initial window at this time.
     */
    public long startWindowMillis;

    public boolean deployBufferServer = true;

    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
              .append("applicationAttributes", this.applicationAttributes).toString();
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
    private ArrayList<OperatorStats> windowStats = new ArrayList<OperatorStats>();

    /**
     * The operator stats for the windows processed during the heartbeat interval.
     * @return ArrayList<OperatorStats>
     */
    public ArrayList<OperatorStats> getWindowStats() {
      return windowStats;
    }

    /**
     * The operator stats for the windows processed during the heartbeat interval.
     * @param stats
     */
    public void setWindowStats(ArrayList<OperatorStats> stats) {
      this.windowStats = stats;
    }

    public static enum DNodeState {
      NEW, // node instantiated but not processing yet
      ACTIVE,
      IDLE,// the node stopped processing (no more input etc.)
      FAILED // problemo!
    }

    /**
     * The originating node. There can be multiple operators in a container.
     */
    private int nodeId;

    public int getNodeId() {
      return nodeId;
    }

    public void setNodeId(int nodeId) {
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
     * State of the operator (processing, idle etc).
     */
    private String state;

    public String getState() {
      return state;
    }

    public void setState(String state) {
      this.state = state;
    }

    private long lastBackupWindowId;

    public long getLastBackupWindowId() {
      return lastBackupWindowId;
    }

    public void setLastBackupWindowId(long lastBackupWindowId) {
      this.lastBackupWindowId = lastBackupWindowId;
    }

    private String recordingName;

    public String getRecordingName() {
      return recordingName;
    }

    public void setRecordingName(String recordingName) {
      this.recordingName = recordingName;
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

    /**
     * Buffer server address for this container.
     * Port numbers are dynamically assigned and the master uses this info to deploy subscribers.
     */
    public String bufferServerHost;
    public int bufferServerPort;


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
      CHECKPOINT, START_RECORDING, STOP_RECORDING, SYNC_RECORDING, SET_PROPERTY
    }

    private int operatorId;
    private RequestType requestType;
    private long recoveryCheckpoint;
    private String portName;

    public String setPropertyKey;
    public String setPropertyValue;

    public int getOperatorId() {
      return operatorId;
    }

    public void setOperatorId(int id) {
      this.operatorId = id;
    }

    public RequestType getRequestType() {
      return requestType;
    }

    public void setRequestType(RequestType requestType) {
      this.requestType = requestType;
    }

    public long getRecoveryCheckpoint() {
      return recoveryCheckpoint;
    }

    public void setRecoveryCheckpoint(long recoveryCheckpoint) {
      this.recoveryCheckpoint = recoveryCheckpoint;
    }

    public String getPortName() {
      return portName;
    }

    public void setPortName(String portName) {
      this.portName = portName;
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
              .append("operatorId", this.operatorId)
              .append("requestType", this.requestType)
              .append("portName", this.portName).toString();
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
    public List<OperatorDeployInfo> undeployRequest;

    /**
     * Set when new operators need to be deployed
     */
    public List<OperatorDeployInfo> deployRequest;

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

}
