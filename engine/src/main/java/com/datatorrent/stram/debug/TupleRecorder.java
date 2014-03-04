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
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.*;
import com.datatorrent.api.codec.JsonStreamCodec;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.common.util.Slice;
import com.datatorrent.stram.engine.WindowGenerator;
import com.datatorrent.stram.tuple.Tuple;
import com.datatorrent.stram.util.FSPartFileCollection;
import com.datatorrent.stram.util.SharedPubSubWebSocketClient;
import com.datatorrent.stram.util.SharedPubSubWebSocketClient.Handler;

/**
 * <p>
 * TupleRecorder class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.2
 */
public class TupleRecorder
{
  public static final String VERSION = "1.2";
  private int totalTupleCount = 0;
  private final HashMap<String, PortInfo> portMap = new HashMap<String, PortInfo>(); // used for output portInfo <name, id> map
  private final HashMap<String, PortCount> portCountMap = new HashMap<String, PortCount>(); // used for tupleCount of each port <name, count> map
  private transient long currentWindowId = WindowGenerator.MIN_WINDOW_ID - 1;
  private transient ArrayList<Range> windowIdRanges = new ArrayList<Range>();
  private long startTime = System.currentTimeMillis();
  private String containerId;
  private int nextPortIndex = 0;
  private final HashMap<String, Sink<Object>> sinks = new HashMap<String, Sink<Object>>();
  private transient long endWindowTuplesProcessed = 0;
  private transient StreamCodec<Object> streamCodec;
  private int numSubscribers = 0;
  private SharedPubSubWebSocketClient wsClient;
  private String recordingNameTopic;
  private final FSPartFileCollection storage = new FSPartFileCollection()
  {
    @Override
    protected String getIndexExtraInfo()
    {
      if (windowIdRanges.isEmpty()) {
        return null;
      }
      String str;
      windowIdRanges.get(windowIdRanges.size() - 1).high = TupleRecorder.this.currentWindowId;
      str = convertToString(windowIdRanges);
      int i = 0;
      str += ":";
      StringBuilder countStr = new StringBuilder("{");
      for (PortCount pc: portCountMap.values()) {
        if (i != 0) {
          countStr.append(",");
        }
        countStr.append("\"").append(pc.id).append("\":\"").append(pc.count).append("\"");
        i++;
      }
      countStr.append("}");
      str += countStr.length();
      str += ":" + countStr.toString();
      return str;
    }

    @Override
    protected void resetIndexExtraInfo()
    {
      for (PortCount pc: portCountMap.values()) {
        pc.count = 0;
      }
      windowIdRanges.clear();
    }

  };

  public FSPartFileCollection getStorage()
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

  public void setWebSocketClient(SharedPubSubWebSocketClient wsClient)
  {
    this.wsClient = wsClient;
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
   * @param startTime the startTime to set
   */
  public void setStartTime(long startTime)
  {
    if (this.startTime == Stats.INVALID_TIME_MILLIS) {
      this.startTime = startTime;
    }

    throw new IllegalStateException("Tuple recorder has already started");
  }

  /**
   * @return the containerId
   */
  public String getContainerId()
  {
    return containerId;
  }

  /**
   * @param containerId the containerId to set
   */
  public void setContainerId(String containerId)
  {
    this.containerId = containerId;
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
    public String containerId;
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
    logger.info("Closing down tuple recorder.");
    this.storage.teardown();
  }

  public void setup(Operator operator, Map<Class<?>, Class<? extends StringCodec<?>>> codecs)
  {
    try {
      storage.setup();
      // this is the right place to init startTime, please enable the following call and disable the assignment in the constructor!
      //setStartTime(System.currentTimeMillis());

      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      bos.write((VERSION + "\n").getBytes());

      RecordInfo recordInfo = new RecordInfo();
      recordInfo.startTime = startTime;
      recordInfo.containerId = containerId;

      if (operator != null) {
        BeanInfo beanInfo = Introspector.getBeanInfo(operator.getClass());
        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
        for (PropertyDescriptor pd: propertyDescriptors) {
          String name = pd.getName();
          Method readMethod = pd.getReadMethod();

          if (readMethod != null) {
            readMethod.setAccessible(true);
            recordInfo.properties.put(name, readMethod.invoke(operator));
          }
        }
      }
      streamCodec = new JsonStreamCodec<Object>(codecs);
      Slice f = streamCodec.toByteArray(recordInfo);
      bos.write(f.buffer, f.offset, f.length);
      bos.write("\n".getBytes());

      for (PortInfo pi: portMap.values()) {
        f = streamCodec.toByteArray(pi);
        bos.write(f.buffer, f.offset, f.length);
        bos.write("\n".getBytes());
      }

      storage.writeMetaData(bos.toByteArray());

      if (wsClient != null) {
        recordingNameTopic = "tupleRecorder." + getStartTime();
        setupWsClient();
      }
    }
    catch (Exception ex) {
      logger.error("Trouble setting up tuple recorder", ex);
    }
  }

  private void setupWsClient() throws ExecutionException, IOException, InterruptedException, TimeoutException
  {
    if (wsClient != null) {
      wsClient.addHandler(recordingNameTopic + ".numSubscribers", new Handler()
      {
        @Override
        public void onMessage(String type, String topic, Object data)
        {
          numSubscribers = Integer.valueOf((String)data);
          logger.info("Number of subscribers for recording started at {} is now {}", getStartTime(), numSubscribers);
        }

        @Override
        public void onClose()
        {
          numSubscribers = 0;
        }

      });
    }
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
        storage.writeDataItem(("B:" + System.currentTimeMillis() + ":" + windowId + "\n").getBytes(), false);
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
        storage.writeDataItem(("E:" + System.currentTimeMillis() + ":" + currentWindowId + "\n").getBytes(), false);
        logger.debug("Got last end window tuple.  Flushing...");
        if (!storage.flushData() && wsClient != null) {
          wsClient.publish(SharedPubSubWebSocketClient.LAST_INDEX_TOPIC_PREFIX + ".tuple." + storage.getBasePath(), storage.getLatestIndexLine());
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
      String str = "T:" + System.currentTimeMillis() + ":" + pi.id + ":" + f.length + ":";
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
      String str = "C:" + System.currentTimeMillis() + ":" + pi.id + ":" + f.length + ":";
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
    for (Range range: ranges) {
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
      if (wsClient != null && wsClient.isConnectionOpen()) {
        HashMap<String, Object> map = new HashMap<String, Object>();
        map.put("portId", String.valueOf(portId));
        map.put("windowId", currentWindowId);
        map.put("tupleCount", totalTupleCount);
        map.put("data", obj);
        wsClient.publish(recordingNameTopic, map);
      }
    }
    catch (Exception ex) {
      logger.warn("Error publishing tuple data", ex);
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

  private static final Logger logger = LoggerFactory.getLogger(TupleRecorder.class);
}
