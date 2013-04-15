/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.Operator;
import com.malhartech.api.Sink;
import com.malhartech.api.StreamCodec;
import com.malhartech.bufferserver.packet.MessageType;
import com.malhartech.engine.Tuple;
import com.malhartech.util.PubSubWebSocketClient;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import com.malhartech.netlet.Client.Fragment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.eclipse.jetty.websocket.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class TupleRecorder implements Operator
{
  public static final String INDEX_FILE = "index.txt";
  public static final String META_FILE = "meta.txt";
  public static final String VERSION = "1.0";
  private transient FileSystem fs;
  private transient FSDataOutputStream partOutStr;
  private transient FSDataOutputStream indexOutStr;
  private transient OutputStream localDataOutput;
  private transient OutputStream localIndexOutput;
  private transient String localBasePath;
  private int bytesPerPartFile = 100 * 1024;
  private long millisPerPartFile = 30 * 60 * 1000; // 30 minutes
  private String basePath = ".";
  private transient String hdfsFile;
  private int fileParts = 0;
  private int partFileTupleCount = 0;
  private int totalTupleCount = 0;
  private long currentPartFileTimeStamp = 0;
  private HashMap<String, PortInfo> portMap = new HashMap<String, PortInfo>(); // used for output portInfo <name, id> map
  private HashMap<String, PortCount> portCountMap = new HashMap<String, PortCount>(); // used for tupleCount of each port <name, count> map
  private transient long currentWindowId = -1;
  private transient ArrayList<Range> windowIdRanges = new ArrayList<Range>();
  //private transient long partBeginWindowId = -1;
  private String recordingName = "Untitled";
  private final long startTime = System.currentTimeMillis();
  private int nextPortIndex = 0;
  private HashMap<String, Sink<Object>> sinks = new HashMap<String, Sink<Object>>();
  private transient long endWindowTuplesProcessed = 0;
  private boolean isLocalMode = false;
  @SuppressWarnings("rawtypes")
  private Class<? extends StreamCodec> streamCodecClass = JsonStreamCodec.class;
  private transient StreamCodec<Object> streamCodec;
  private static final Logger logger = LoggerFactory.getLogger(TupleRecorder.class);
  private boolean syncRequested = false;
  private URI pubSubUrl = null;
  private int numSubscribers = 0;
  private PubSubWebSocketClient wsClient;
  private String recordingNameTopic;

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
   * @param streamCodecClass
   */
  public void setStreamCodec(Class<? extends StreamCodec<Object>> streamCodecClass)
  {
    this.streamCodecClass = streamCodecClass;
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

  public void setLocalMode(boolean isLocalMode)
  {
    this.isLocalMode = isLocalMode;
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
    public String recordingName;
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

  public String getRecordingName()
  {
    return recordingName;
  }

  public void setRecordingName(String recordingName)
  {
    this.recordingName = recordingName;
  }

  public void setBytesPerPartFile(int bytes)
  {
    this.bytesPerPartFile = bytes;
  }

  public void setMillisPerPartFile(long millis)
  {
    this.millisPerPartFile = millis;
  }

  public void setBasePath(String path)
  {
    this.basePath = path;
  }

  public String getBasePath()
  {
    return basePath;
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

  @Override
  public void teardown()
  {
    logger.info("Closing down tuple recorder.");
    try {
      if (partOutStr != null) {
        logger.debug("Closing part file");
        partOutStr.close();
        if (indexOutStr != null) {
          writeIndex();
          writeIndexEnd();
        }
      }
      if (indexOutStr != null) {
        indexOutStr.close();
      }
    }
    catch (IOException ex) {
      logger.error(ex.toString());
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setup(OperatorContext context)
  {
    try {
      streamCodec = streamCodecClass.newInstance();
      Path pa = new Path(basePath, META_FILE);
      if (basePath.startsWith("file:")) {
        isLocalMode = true;
        localBasePath = basePath.substring(5);
        (new File(localBasePath)).mkdirs();
      }
      fs = FileSystem.get(pa.toUri(), new Configuration());
      FSDataOutputStream metaOs = fs.create(pa);

      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      bos.write((VERSION + "\n").getBytes());

      RecordInfo recordInfo = new RecordInfo();
      recordInfo.startTime = startTime;
      recordInfo.recordingName = recordingName;
      Fragment f = streamCodec.toByteArray(recordInfo).data;
      bos.write(f.buffer, f.offset, f.length);
      bos.write("\n".getBytes());

      for (PortInfo pi: portMap.values()) {
        f = streamCodec.toByteArray(pi).data;
        bos.write(f.buffer, f.offset, f.length);
        bos.write("\n".getBytes());
      }

      metaOs.write(bos.toByteArray());
      metaOs.hflush();
      metaOs.close();

      pa = new Path(basePath, INDEX_FILE);
      if (isLocalMode) {
        localIndexOutput = new FileOutputStream(localBasePath + "/" + INDEX_FILE);
        indexOutStr = new FSDataOutputStream(localIndexOutput, null);
      }
      else {
        indexOutStr = fs.create(pa);
      }
      if (pubSubUrl != null) {
        recordingNameTopic = "tupleRecorder." + recordingName;
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
          logger.info("Number of subscribers for {} is now {}", recordingName, numSubscribers);
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

  protected void openNewPartFile() throws IOException
  {
    hdfsFile = "part" + fileParts + ".txt";
    Path path = new Path(basePath, hdfsFile);
    logger.debug("Opening new part file: {}", hdfsFile);
    if (isLocalMode) {
      localDataOutput = new FileOutputStream(localBasePath + "/" + hdfsFile);
      partOutStr = new FSDataOutputStream(localDataOutput, null);
    }
    else {
      partOutStr = fs.create(path);
    }
    fileParts++;
    currentPartFileTimeStamp = System.currentTimeMillis();
    partFileTupleCount = 0;
  }

  @Override
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
        if (partOutStr == null) {
          openNewPartFile();
        }
        logger.debug("Writing begin window (id: {}) to tuple recorder", windowId);
        partOutStr.write(("B:" + windowId + "\n").getBytes());
        //fsOutput.hflush();
      }
      catch (IOException ex) {
        logger.error(ex.toString());
      }
    }
  }

  @Override
  public void endWindow()
  {
    if (++endWindowTuplesProcessed == portMap.size()) {
      try {
        partOutStr.write(("E:" + currentWindowId + "\n").getBytes());
        logger.debug("Got last end window tuple.  Flushing...");
        partOutStr.hflush();
        if (syncRequested || (partOutStr.getPos() > bytesPerPartFile) || (currentPartFileTimeStamp + millisPerPartFile < System.currentTimeMillis())) {
          partOutStr.close();
          partOutStr = null;
          writeIndex();
          syncRequested = false;
          logger.debug("Closing current part file.");
        }
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

  public void requestSync()
  {
    syncRequested = true;
  }

  public void writeTuple(Object obj, String port)
  {
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Fragment f = streamCodec.toByteArray(obj).data;
      bos.write(f.buffer, f.offset, f.length);
      bos.write("\n".getBytes());

      PortInfo pi = portMap.get(port);
      String str = "T:" + pi.id + ":" + bos.size() + ":";
      PortCount pc = portCountMap.get(port);
      pc.count++;
      portCountMap.put(port, pc);

      partOutStr.write(str.getBytes());
      partOutStr.write(bos.toByteArray());
      //logger.debug("Writing tuple for port id {}", pi.id);
      //fsOutput.hflush();
      if (numSubscribers > 0) {
        publishTupleData(pi.id, obj);
      }
      ++partFileTupleCount;
      ++totalTupleCount;
    }
    catch (IOException ex) {
      logger.error(ex.toString());
    }
  }

  public void writeControlTuple(Tuple tuple, String port)
  {
    try {
      PortInfo pi = portMap.get(port);
      String str = "C:" + pi.id; // to be completed when Tuple is externalizable
      if (partOutStr == null) {
        openNewPartFile();
      }
      partOutStr.write(str.getBytes());
      partOutStr.write("\n".getBytes());
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

  public void writeIndex()
  {
    if (windowIdRanges.isEmpty()) {
      return;
    }
    windowIdRanges.get(windowIdRanges.size() - 1).high = this.currentWindowId;
    logger.debug("Writing index file for windows {}", windowIdRanges);
    try {
      indexOutStr.write(("F:" + convertToString(windowIdRanges) + ":T:" + partFileTupleCount + ":").getBytes());

      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      int i = 0;
      String countStr = "{";
      for (String key: portCountMap.keySet()) {
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
      bos.write(countStr.getBytes());
      partFileTupleCount = 0;

      indexOutStr.write((String.valueOf(bos.size()) + ":").getBytes());
      indexOutStr.write(bos.toByteArray());
      indexOutStr.write((":" + hdfsFile + "\n").getBytes());
      indexOutStr.hflush();
      indexOutStr.hsync();
    }
    catch (IOException ex) {
      logger.error(ex.toString());
    }
    windowIdRanges.clear();
  }

  public void writeIndexEnd()
  {
    try {
      indexOutStr.write(("E\n").getBytes());
      indexOutStr.hflush();
      indexOutStr.hsync();
    }
    catch (IOException ex) {
      logger.error(ex.toString());
    }
  }

  private void publishTupleData(int portId, Object obj)
  {
    try {
      if (pubSubUrl != null && wsClient.isConnectionOpen()) {
        HashMap<String, Object> map = new HashMap<String, Object>();
        map.put("portId", String.valueOf(portId));
        map.put("windowId", currentWindowId);
        map.put("data", obj);
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

    public RecorderSink(String portName)
    {
      this.portName = portName;
    }

    @Override
    public void process(Object payload)
    {
      // *** if it's not a control tuple, then (payload instanceof Tuple) returns false
      // In other words, if it's a regular tuple emitted by operators (payload), payload
      // is not an instance of Tuple (confusing... I know)
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

  }

}
