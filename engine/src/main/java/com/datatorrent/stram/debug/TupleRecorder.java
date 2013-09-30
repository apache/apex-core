/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.debug;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.*;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.eclipse.jetty.websocket.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Operator;
import com.datatorrent.api.Sink;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.codec.JsonStreamCodec;
import com.datatorrent.api.util.PubSubWebSocketClient;

import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.common.util.Slice;
import com.datatorrent.stram.engine.Stats;
import com.datatorrent.stram.engine.WindowGenerator;
import com.datatorrent.stram.tuple.Tuple;
import com.datatorrent.stram.util.HdfsPartFileCollection;

/**
 * <p>TupleRecorder class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.2
 */
public class TupleRecorder
{
  public static final String VERSION = "1.1";
  private int totalTupleCount = 0;
  private HashMap<String, PortInfo> portMap = new HashMap<String, PortInfo>(); // used for output portInfo <name, id> map
  private HashMap<String, PortCount> portCountMap = new HashMap<String, PortCount>(); // used for tupleCount of each port <name, count> map
  private transient long currentWindowId = WindowGenerator.MIN_WINDOW_ID - 1;
  private transient ArrayList<Range> windowIdRanges = new ArrayList<Range>();
  private long startTime = Stats.INVALID_TIME_MILLIS;
  private long stopTime = Stats.INVALID_TIME_MILLIS;
  private String recordingName; // should be retired
  private int nextPortIndex = 0;
  private HashMap<String, Sink<Object>> sinks = new HashMap<String, Sink<Object>>();
  private transient long endWindowTuplesProcessed = 0;
  private transient StreamCodec<Object> streamCodec = new JsonStreamCodec<Object>();
  private static final Logger logger = LoggerFactory.getLogger(TupleRecorder.class);
  private URI pubSubUrl = null;
  private int numSubscribers = 0;
  private PubSubWebSocketClient wsClient;
  private String recordingNameTopic;
  private HdfsPartFileCollection storage = new HdfsPartFileCollection()
  {
    @Override
    protected String getAndResetIndexExtraInfo()
    {
      String str;
      windowIdRanges.get(windowIdRanges.size() - 1).high = TupleRecorder.this.currentWindowId;
      str = convertToString(windowIdRanges);
      int i = 0;
      str += ":";
      String countStr;
      countStr = "{";
      for (String key : portCountMap.keySet()) {
        PortCount pc = portCountMap.get(key);
        if (i != 0) {
          countStr += ",";
        }
        countStr += "\"" + pc.id + "\"" + ":\"" + pc.count + "\"";
        i++;
        pc.count = 0;
        portCountMap.put(key, pc);
      }
      countStr += "}";
      str += countStr.length();
      str += ":" + countStr;
      windowIdRanges.clear();
      return str;
    }

  };

  public HdfsPartFileCollection getStorage()
  {
    return storage;
  }

  public RecorderSink newSink(String key)
  {
    RecorderSink recorderSink = new RecorderSink(key);
    sinks.put(key, recorderSink);
    return recorderSink;
  }

  /**
   * Sets the stream codec for serialization to write to the files
   * The serialization method must not produce newlines.
   * For serializations that produces binary, base64 is recommended.
   *
   * @param streamCodec
   */
  public void setStreamCodec(StreamCodec<Object> streamCodec)
  {
    this.streamCodec = streamCodec;
  }

  public void setPubSubUrl(String pubSubUrl) throws URISyntaxException
  {
    this.pubSubUrl = new URI(pubSubUrl);
  }

  public Map<String, PortInfo> getPortInfoMap()
  {
    return Collections.unmodifiableMap(portMap);
  }

  public int getTotalTupleCount()
  {
    return totalTupleCount;
  }

  public Map<String, Sink<Object>> getSinkMap()
  {
    return Collections.unmodifiableMap(sinks);
  }

  /**
   * @return the containerId
   */
  public String getContainerId()
  {
    return "this_container_id_should_be_retired";
  }

  /**
   * @return the recordingName
   */
  public String getRecordingName()
  {
    return recordingName;
  }

  /**
   * @param recordingName the recordingName to set
   */
  public void setRecordingName(String recordingName)
  {
    this.recordingName = recordingName;
  }

  /**
   * @return the stopTime
   */
  public long getStopTime()
  {
    return stopTime;
  }

  /**
   * @param stopTime the stopTime to set
   */
  public void setStopTime(long stopTime)
  {
    if (this.stopTime == Stats.INVALID_TIME_MILLIS || this.stopTime < this.startTime) {
      this.stopTime = stopTime;
    }

    throw new IllegalStateException("Tuple recorder has already stopped");
  }

  /**
   * @param startTime the startTime to set
   */
  public void setStartTime(long startTime)
  {
    if (this.startTime == Stats.INVALID_TIME_MILLIS || this.startTime < this.stopTime) {
      this.startTime = startTime;
    }

    throw new IllegalStateException("Tuple recorder has already started");
  }

  /* defined for json information */
  public static class PortInfo
  {
    public String name;
    public String streamName;
    public String type;
    public int id;
  }

  /* defined for written tuple count of each port recorded in index file */
  public static class PortCount
  {
    public int id;
    public long count;
  }

  public static class RecordInfo
  {
    public long startTime;
    public long stopTime;
    public String recordingName;
    public Map<String, Object> properties = new HashMap<String, Object>();
  }

  public static class Range
  {
    public long low = -1;
    public long high = -1;

    public Range()
    {
    }

    public Range(long low, long high)
    {
      this.low = low;
      this.high = high;
    }

    @Override
    public String toString()
    {
      return "[" + String.valueOf(low) + "," + String.valueOf(high) + "]";
    }

  }

  public long getStartTime()
  {
    return startTime;
  }

  public void addInputPortInfo(String portName, String streamName)
  {
    PortInfo portInfo = new PortInfo();
    portInfo.name = portName;
    portInfo.streamName = streamName;
    portInfo.type = "input";
    portInfo.id = nextPortIndex++;
    portMap.put(portName, portInfo);
    PortCount pc = new PortCount();
    pc.id = portInfo.id;
    pc.count = 0;
    portCountMap.put(portName, pc);
  }

  public void addOutputPortInfo(String portName, String streamName)
  {
    PortInfo portInfo = new PortInfo();
    portInfo.name = portName;
    portInfo.streamName = streamName;
    portInfo.type = "output";
    portInfo.id = nextPortIndex++;
    portMap.put(portName, portInfo);
    PortCount pc = new PortCount();
    pc.id = portInfo.id;
    pc.count = 0;
    portCountMap.put(portName, pc);
  }

  public void teardown()
  {
    stopTime = System.currentTimeMillis();
    logger.info("Closing down tuple recorder.");
    this.storage.teardown();
  }

  @SuppressWarnings("unchecked")
  public void setup(Operator operator)
  {
    try {
      storage.setup();
      startTime = System.currentTimeMillis();

      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      bos.write((VERSION + "\n").getBytes());

      RecordInfo recordInfo = new RecordInfo();
      recordInfo.startTime = startTime;
      recordInfo.stopTime = stopTime;
      recordInfo.recordingName = recordingName;

      if (operator != null) {
        BeanInfo beanInfo = Introspector.getBeanInfo(operator.getClass());
        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
        for (PropertyDescriptor pd : propertyDescriptors) {
          String name = pd.getName();
          Method readMethod = pd.getReadMethod();

          if (readMethod != null) {
            readMethod.setAccessible(true);
            recordInfo.properties.put(name, readMethod.invoke(operator));
          }
        }
      }

      Slice f = streamCodec.toByteArray(recordInfo);
      bos.write(f.buffer, f.offset, f.length);
      bos.write("\n".getBytes());

      for (PortInfo pi : portMap.values()) {
        f = streamCodec.toByteArray(pi);
        bos.write(f.buffer, f.offset, f.length);
        bos.write("\n".getBytes());
      }

      storage.writeMetaData(bos.toByteArray());

      if (pubSubUrl != null) {
        recordingNameTopic = "tupleRecorder." + getStartTime();
        try {
          setupWsClient();
        }
        catch (Exception ex) {
          logger.error("Cannot connect to daemon at {}", pubSubUrl);
        }
      }
    }
    catch (Exception ex) {
      logger.error("Trouble setting up tuple recorder", ex);
    }
  }

  private void setupWsClient() throws ExecutionException, IOException, InterruptedException, TimeoutException
  {
    wsClient = new PubSubWebSocketClient()
    {
      @Override
      public void onOpen(WebSocket.Connection connection)
      {
      }

      @Override
      public void onMessage(String type, String topic, Object data)
      {
        if (topic.equals(recordingNameTopic + ".numSubscribers")) {
          numSubscribers = Integer.valueOf((String)data);
          logger.info("Number of subscribers for recording started at {} is now {}", getStartTime(), numSubscribers);
        }
      }

      @Override
      public void onClose(int code, String message)
      {
        numSubscribers = 0;
      }

    };
    wsClient.setUri(pubSubUrl);
    wsClient.openConnection(500);
    wsClient.subscribeNumSubscribers(recordingNameTopic);
  }

  public void beginWindow(long windowId)
  {
    if (this.currentWindowId != windowId) {
      if (windowId != this.currentWindowId + 1) {
        if (!windowIdRanges.isEmpty()) {
          windowIdRanges.get(windowIdRanges.size() - 1).high = this.currentWindowId;
        }
        Range range = new Range();
        range.low = windowId;
        windowIdRanges.add(range);
      }
      if (windowIdRanges.isEmpty()) {
        Range range = new Range();
        range.low = windowId;
        windowIdRanges.add(range);
      }
      this.currentWindowId = windowId;
      endWindowTuplesProcessed = 0;
      try {
        storage.writeDataItem(("B:" + windowId + "\n").getBytes(), false);
      }
      catch (IOException ex) {
        logger.error(ex.toString());
      }
    }
  }

  public void endWindow()
  {
    if (++endWindowTuplesProcessed == portMap.size()) {
      try {
        storage.writeDataItem(("E:" + currentWindowId + "\n").getBytes(), false);
        logger.debug("Got last end window tuple.  Flushing...");
        storage.flushData();
        if (pubSubUrl != null) {
          // check web socket connection
          if (!wsClient.isConnectionOpen()) {
            try {
              setupWsClient();
            }
            catch (Exception ex) {
              logger.error("Cannot connect to daemon");
            }
          }
        }
      }
      catch (IOException ex) {
        logger.error("Exception caught in endWindow", ex);
      }
    }
  }

  public void writeTuple(Object obj, String port)
  {
    if (windowIdRanges.isEmpty()) {
      throw new RuntimeException("Data tuples received from tuple recorder before any BEGIN_WINDOW");
    }
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Slice f = streamCodec.toByteArray(obj);
      PortInfo pi = portMap.get(port);
      String str = "T:" + pi.id + ":" + f.length + ":";
      bos.write(str.getBytes());
      bos.write(f.buffer, f.offset, f.length);
      bos.write("\n".getBytes());
      PortCount pc = portCountMap.get(port);
      pc.count++;
      portCountMap.put(port, pc);

      storage.writeDataItem(bos.toByteArray(), true);
      //logger.debug("Writing tuple for port id {}", pi.id);
      //fsOutput.hflush();
      ++totalTupleCount;
      if (numSubscribers > 0) {
        // this is not asynchronous.  we need to fix this
        publishTupleData(pi.id, obj);
      }
    }
    catch (IOException ex) {
      logger.error(ex.toString());
    }
  }

  public void writeControlTuple(Tuple tuple, String port)
  {
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      PortInfo pi = portMap.get(port);
      Slice f = streamCodec.toByteArray(tuple);
      String str = "C:" + pi.id + ":" + f.length + ":";
      bos.write(str.getBytes());
      bos.write(f.buffer, f.offset, f.length);
      bos.write("\n".getBytes());
      storage.writeDataItem(bos.toByteArray(), false);
    }
    catch (IOException ex) {
      logger.error(ex.toString());
    }
  }

  private static String convertToString(List<Range> ranges)
  {
    String result = "";
    int i = 0;
    for (Range range : ranges) {
      if (i++ > 0) {
        result += ",";
      }
      result += String.valueOf(range.low);
      result += "-";
      result += String.valueOf(range.high);
    }
    return result;
  }

  private void publishTupleData(int portId, Object obj)
  {
    try {
      if (pubSubUrl != null && wsClient.isConnectionOpen()) {
        HashMap<String, Object> map = new HashMap<String, Object>();
        map.put("portId", String.valueOf(portId));
        map.put("windowId", currentWindowId);
        map.put("tupleCount", totalTupleCount);
        map.put("data", obj);
        // this is NOT asynchronous.  We need to fix this!
        wsClient.publish(recordingNameTopic, map);
      }
    }
    catch (Exception ex) {
      logger.warn("Error posting to URL {}", pubSubUrl, ex);
    }
  }

  public class RecorderSink implements Sink<Object>
  {
    private final String portName;
    private int count;

    public RecorderSink(String portName)
    {
      this.portName = portName;
    }

    @Override
    public void put(Object payload)
    {
      // *** if it's not a control tuple, then (payload instanceof Tuple) returns false
      // In other words, if it's a regular tuple emitted by operators (payload), payload
      // is not an instance of Tuple (confusing... I know)
      ++count;
      if (payload instanceof Tuple) {
        Tuple tuple = (Tuple)payload;
        MessageType messageType = tuple.getType();
        if (messageType == MessageType.BEGIN_WINDOW) {
          beginWindow(tuple.getWindowId());
        }
        writeControlTuple(tuple, portName);
        if (messageType == MessageType.END_WINDOW) {
          endWindow();
        }
      }
      else {
        writeTuple(payload, portName);
      }
    }

    @Override
    public int getCount(boolean reset)
    {
      try {
        return count;
      }
      finally {
        if (reset) {
          count = 0;
        }
      }
    }

  }

}
