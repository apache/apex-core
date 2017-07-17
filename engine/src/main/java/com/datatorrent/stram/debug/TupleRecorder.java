/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.debug;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.codehaus.jackson.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.common.util.PropertiesHelper;

import com.datatorrent.api.Operator;
import com.datatorrent.api.Sink;
import com.datatorrent.api.Stats;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.StringCodec;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.common.codec.JsonStreamCodec;
import com.datatorrent.common.util.ObjectMapperString;
import com.datatorrent.netlet.util.Slice;
import com.datatorrent.stram.engine.WindowGenerator;
import com.datatorrent.stram.tuple.Tuple;
import com.datatorrent.stram.util.FSPartFileCollection;
import com.datatorrent.stram.util.SharedPubSubWebSocketClient;
import com.datatorrent.stram.util.SharedPubSubWebSocketClient.Handler;

/**
 * <p>
 * TupleRecorder class.</p>
 *
 * @since 0.3.2
 */
public class TupleRecorder
{
  public static final String VERSION = "1.2";
  private long totalTupleCount = 0;
  private final HashMap<String, PortInfo> portMap = new HashMap<>(); // used for output portInfo <name, id> map
  private final HashMap<String, PortCount> portCountMap = new HashMap<>(); // used for tupleCount of each port <name, count> map
  private transient long currentWindowId = WindowGenerator.MIN_WINDOW_ID - 1;
  private transient ArrayList<Range> windowIdRanges = new ArrayList<>();
  private long startTime = Stats.INVALID_TIME_MILLIS;
  private String id;
  private final String appId;
  private int nextPortIndex = 0;
  private final HashMap<String, Sink<Object>> sinks = new HashMap<>();
  private transient long endWindowTuplesProcessed = 0;
  private transient StreamCodec<Object> streamCodec;
  private int numSubscribers = 0;
  private SharedPubSubWebSocketClient wsClient;
  private String recordingNameTopic;
  private long numWindows = Long.MAX_VALUE; // number of windows to record
  private Runnable stopProcedure; // stop procedure to execute

  private static final Logger logger = LoggerFactory.getLogger(TupleRecorder.class);

  // If there are errors processing tuples, don't log an error for every tuple as it could overwhelm the logs.
  // The property specifies the minumum number of tuples between two consecutive error log statements. Set it to zero to
  // log every tuple error
  private static long ERROR_LOG_GAP = PropertiesHelper.getLong("org.apache.apex.stram.tupleRecorder.errorLogGap", 10000L, 0, Long.MAX_VALUE);
  long lastLog = -1;

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

  public TupleRecorder(String id, String appId)
  {
    this.id = id;
    this.appId = appId;
  }

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

  public long getTotalTupleCount()
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
    } else {
      throw new IllegalStateException("Tuple recorder has already started at " + this.startTime);
    }
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
    public String appId;
    public Map<String, ObjectMapperString> properties = new HashMap<>();
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

  public String getId()
  {
    return id;
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
      setStartTime(System.currentTimeMillis());
      if (id == null) {
        id = String.valueOf(startTime);
      }

      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      bos.write((VERSION + "\n").getBytes());

      RecordInfo recordInfo = new RecordInfo();
      recordInfo.startTime = startTime;
      recordInfo.appId = appId;

      streamCodec = new JsonStreamCodec<>(codecs);

      if (operator != null) {
        BeanInfo beanInfo = Introspector.getBeanInfo(operator.getClass());
        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
        for (PropertyDescriptor pd: propertyDescriptors) {
          String name = pd.getName();
          Method readMethod = pd.getReadMethod();

          if (readMethod != null) {
            readMethod.setAccessible(true);
            try {
              Slice f = streamCodec.toByteArray(readMethod.invoke(operator));
              recordInfo.properties.put(name, new ObjectMapperString(f.stringValue()));
            } catch (Throwable t) {
              logger.warn("Cannot serialize property {} for operator {}", name, operator.getClass());
              recordInfo.properties.put(name, null);
            }
          }
        }
      }
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
        recordingNameTopic = "applications." + appId + ".tupleRecorder." + getStartTime();
        setupWsClient();
      }
    } catch (Exception ex) {
      logger.error("Trouble setting up tuple recorder", ex);
    }
  }

  private void setupWsClient() throws ExecutionException, IOException, InterruptedException, TimeoutException
  {
    if (wsClient != null) {
      wsClient.addHandler(recordingNameTopic, true, new Handler()
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
      } catch (IOException ex) {
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
      } catch (IOException ex) {
        logger.error("Exception caught in endWindow", ex);
      }
    }
    if (stopProcedure != null && --numWindows <= 0) {
      stopProcedure.run();
    }
  }

  public void writeTuple(Object obj, String port)
  {
    ++totalTupleCount;
    if (windowIdRanges.isEmpty()) {
      throw new RuntimeException("Data tuples received from tuple recorder before any BEGIN_WINDOW");
    }
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Slice f = null;
    try {
      f = streamCodec.toByteArray(obj);
    } catch (RuntimeException ex) {
      checkLogTuple(ex, "save", obj);
      return;
    }
    try {
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
      if (numSubscribers > 0) {
        // this is not asynchronous.  we need to fix this
        publishTupleData(pi.id, obj);
      }
    } catch (Exception ex) {
      logger.warn("Error saving tuple", ex);
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
    } catch (IOException ex) {
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
        HashMap<String, Object> map = new HashMap<>();
        map.put("portId", String.valueOf(portId));
        map.put("windowId", currentWindowId);
        map.put("tupleCount", totalTupleCount);
        map.put("data", obj);
        wsClient.publish(recordingNameTopic, map);
      }
    } catch (Exception ex) {
      if (ex instanceof JsonProcessingException) {
        checkLogTuple(ex, "publish", obj);
      } else {
        logger.warn("Error publishing tuple", ex);
      }
    }
  }

  private void checkLogTuple(Exception ex, String context, Object tuple)
  {
    if ((lastLog == -1) || (totalTupleCount - lastLog) >= ERROR_LOG_GAP) {
      lastLog = totalTupleCount;
      logger.warn("Error serializing during {} for tuple {} ", context, tuple, ex);
    }
  }

  public void setNumWindows(long numWindows, Runnable stopProcedure)
  {
    this.numWindows = numWindows;
    this.stopProcedure = stopProcedure;
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
      } else {
        writeTuple(payload, portName);
      }
    }

    @Override
    public int getCount(boolean reset)
    {
      try {
        return count;
      } finally {
        if (reset) {
          count = 0;
        }
      }
    }

  }

}
