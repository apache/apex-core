/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.Operator;
import com.malhartech.api.Sink;
import com.malhartech.engine.Tuple;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class TupleRecorder implements Operator
{
  public static final String INDEX_FILE = "index.txt";
  public static final String META_FILE = "meta.txt";
  public static final String VERSION = "1.0";
  private transient FileSystem fs;
  private transient FSDataOutputStream fsOutput;
  private transient FSDataOutputStream indexOs;
  private int bytesPerFile = 100 * 1024;
  private String basePath = ".";
  private transient String hdfsFile;
  private int fileParts = 0;
  private int tupleCount = 0;
  private HashMap<String, PortInfo> portMap = new HashMap<String, PortInfo>(); // used for output portInfo <name, id> map
  private transient long windowId;
  private String recordingName = "Untitled";
  private final long startTime = System.currentTimeMillis();
  private int nextPortIndex = 0;
  private HashMap<String, Sink<Object>> sinks = new HashMap<String, Sink<Object>>();
  private transient long endWindowTuplesProcessed = 0;
  private boolean isLocalMode = false;
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(TupleRecorder.class);
  private long indexBeginWindowId;

  public RecorderSink newSink(String key)
  {
    RecorderSink recorderSink = new RecorderSink(key);
    sinks.put(key, recorderSink);
    return recorderSink;
  }

  public HashMap<String, PortInfo> getPortInfoMap()
  {
    return portMap;
  }

  public HashMap<String, Sink<Object>> getSinkMap()
  {
    return sinks;
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
  /* defined for json information */

  public static class RecordInfo
  {
    public long startTime;
    public String recordingName;
  }

  public String getRecordingName()
  {
    return recordingName;
  }

  public void setRecordingName(String recordingName)
  {
    this.recordingName = recordingName;
  }

  public void setBytesPerFile(int bytes)
  {
    bytesPerFile = bytes;
  }

  public void setBasePath(String path)
  {
    basePath = path;
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
  }

  public void addOutputPortInfo(String portName, String streamName)
  {
    PortInfo portInfo = new PortInfo();
    portInfo.name = portName;
    portInfo.streamName = streamName;
    portInfo.type = "output";
    portInfo.id = nextPortIndex++;
    portMap.put(portName, portInfo);
  }

  @Override
  public void teardown()
  {
    if (indexOs != null) {
      try {
        indexOs.close();
      }
      catch (IOException ex) {
        logger.error(ex.toString());
      }
    }
    if (fsOutput != null) {
      try {
        fsOutput.close();
      }
      catch (IOException ex) {
        logger.error(ex.toString());
      }
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      Path pa = new Path(basePath, META_FILE);
      if (isLocalMode) {
        fs = LocalFileSystem.get(pa.toUri(), new Configuration());
        System.out.println(pa.toUri().toString());
      }
      else {
        fs = FileSystem.get(pa.toUri(), new Configuration());
      }
      FSDataOutputStream metaOs = fs.create(pa);

      ObjectMapper mapper = new ObjectMapper();
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      bos.write((VERSION + "\n").getBytes());

      RecordInfo recordInfo = new RecordInfo();
      recordInfo.startTime = startTime;
      recordInfo.recordingName = recordingName;
      mapper.writeValue(bos, recordInfo);
      bos.write("\n".getBytes());

      for (PortInfo pi: portMap.values()) {
        mapper.writeValue(bos, pi);
        bos.write("\n".getBytes());
      }

      metaOs.write(bos.toByteArray());
      metaOs.hflush();
      metaOs.close();

      pa = new Path(basePath, INDEX_FILE);
//      indexOs = fs.create(pa, true, 10);
      indexOs = fs.create(pa);
    }
    catch (IOException ex) {
      logger.error(ex.toString());
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    if (this.windowId != windowId) {
      this.windowId = windowId;
      endWindowTuplesProcessed = 0;
      try {
        if (fsOutput == null || fsOutput.getPos() > bytesPerFile) {
          hdfsFile = "part" + fileParts + ".txt";
          Path path = new Path(basePath, hdfsFile);
          logger.info("Opening new part file: {}", hdfsFile);
          fsOutput = fs.create(path);
          fileParts++;
          indexBeginWindowId = windowId;
        }
        logger.info("Writing begin window (id: {}) to tuple recorder", windowId);
        fsOutput.write(("B:" + windowId + "\n").getBytes());
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
        fsOutput.write(("E:" + windowId + "\n").getBytes());
        logger.info("Got last end window tuple.  Flushing...");
        fsOutput.hflush();
        fsOutput.flush();
        if (fsOutput.getPos() > bytesPerFile) {
          fsOutput.close();
          indexOs.write(("F:" + ":" + indexBeginWindowId + ":" + windowId + ":T:" + tupleCount + ":" + hdfsFile + "\n").getBytes());
          indexOs.hflush();
          indexOs.flush();
          logger.info("Closing current part file because it's full");
        }
      }
      catch (JsonGenerationException ex) {
        logger.error(ex.toString());
      }
      catch (JsonMappingException ex) {
        logger.error(ex.toString());
      }
      catch (IOException ex) {
        logger.error(ex.toString());
      }
    }
  }

  public void writeTuple(Object obj, String port)
  {
    try {
      ObjectMapper mapper = new ObjectMapper();
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      mapper.writeValue(bos, obj);
      bos.write("\n".getBytes());

      PortInfo pi = portMap.get(port);
      String str = "T:" + pi.id + ":" + bos.size() + ":";
      fsOutput.write(str.getBytes());
      fsOutput.write(bos.toByteArray());
      logger.info("Writing tuple for port id {}", pi.id);
      //fsOutput.hflush();
      ++tupleCount;
    }
    catch (JsonGenerationException ex) {
      logger.error(ex.toString());
    }
    catch (JsonMappingException ex) {
      logger.error(ex.toString());
    }
    catch (IOException ex) {
      logger.error(ex.toString());
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
        switch (tuple.getType()) {
          case BEGIN_WINDOW:
            beginWindow(tuple.getWindowId());
            break;

          case END_WINDOW:
            endWindow();
            break;

          case PAYLOAD:
            writeTuple(payload, portName);
            break;
        }
      }
      else {
        //writeTuple(payload, portName);
      }
    }

  }

}
