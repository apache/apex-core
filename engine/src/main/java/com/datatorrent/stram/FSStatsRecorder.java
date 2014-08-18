/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.RecordField;
import com.datatorrent.common.util.Slice;
import com.datatorrent.lib.codec.JsonStreamCodec;
import com.datatorrent.stram.util.FSPartFileCollection;
import com.datatorrent.stram.webapp.ContainerInfo;
import com.datatorrent.stram.webapp.OperatorInfo;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>FSStatsRecorder class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.2
 */
public class FSStatsRecorder implements StatsRecorder
{
  public static final String VERSION = "1.0";
  private static final Logger LOG = LoggerFactory.getLogger(FSStatsRecorder.class);
  private String basePath = ".";
  private FSPartFileCollection containersStorage;
  private final Map<String, FSPartFileCollection> logicalOperatorStorageMap = new ConcurrentHashMap<String, FSPartFileCollection>();
  private final Map<String, Integer> knownContainers = new HashMap<String, Integer>();
  private final Set<String> knownOperators = new HashSet<String>();
  private transient StreamCodec<Object> streamCodec;
  private final Map<Class<?>, List<Field>> metaFields = new HashMap<Class<?>, List<Field>>();
  private final Map<Class<?>, List<Field>> statsFields = new HashMap<Class<?>, List<Field>>();
  private final BlockingQueue<WriteOperation> queue = new LinkedBlockingQueue<WriteOperation>();
  private final StatsRecorderThread statsRecorderThread = new StatsRecorderThread();

  private class StatsRecorderThread extends Thread
  {
    @Override
    public void run()
    {
      while (true) {
        try {
          WriteOperation wo = queue.take();
          if (wo.meta) {
            wo.storage.writeMetaData(wo.bytes);
          }
          else {
            wo.storage.writeDataItem(wo.bytes, true);
          }
          Thread.yield();
          if (queue.isEmpty()) {
            containersStorage.flushData();
            for (FSPartFileCollection operatorStorage : logicalOperatorStorageMap.values()) {
              operatorStorage.flushData();
            }
          }
        }
        catch (InterruptedException ex) {
          return;
        }
        catch (Exception ex) {
          LOG.error("Caught Exception", ex);
        }
      }
    }

  }

  private static class WriteOperation {
    WriteOperation(FSPartFileCollection storage, byte[] bytes, boolean meta) {
      this.storage = storage;
      this.bytes = bytes;
      this.meta = meta;
    }
    FSPartFileCollection storage;
    byte[] bytes;
    boolean meta;
  }

  public void setBasePath(String basePath)
  {
    this.basePath = basePath;
  }

  public void setup()
  {
    try {
      streamCodec = new JsonStreamCodec<Object>();
      containersStorage = new FSPartFileCollection();
      containersStorage.setBasePath(basePath + "/containers");
      containersStorage.setup();
      containersStorage.writeMetaData((VERSION + "\n").getBytes());
      statsRecorderThread.start();
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public void teardown()
  {
    statsRecorderThread.interrupt();
  }

  @Override
  public void recordContainers(Map<String, StreamingContainerAgent> containerMap, long timestamp) throws IOException
  {
    for (Map.Entry<String, StreamingContainerAgent> entry : containerMap.entrySet()) {
      StreamingContainerAgent sca = entry.getValue();
      ContainerInfo containerInfo = sca.getContainerInfo();
      if (!containerInfo.state.equals("ACTIVE")) {
        continue;
      }
      int containerIndex;
      if (!knownContainers.containsKey(entry.getKey())) {
        containerIndex = knownContainers.size();
        knownContainers.put(entry.getKey(), containerIndex);
        Map<String, Object> fieldMap = extractRecordFields(containerInfo, "meta");
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Slice f = streamCodec.toByteArray(fieldMap);
        bos.write((String.valueOf(containerIndex) + ":").getBytes());
        bos.write(f.buffer, f.offset, f.length);
        bos.write("\n".getBytes());
        queue.add(new WriteOperation(containersStorage, bos.toByteArray(), true));
      }
      else {
        containerIndex = knownContainers.get(entry.getKey());
      }
      Map<String, Object> fieldMap = extractRecordFields(containerInfo, "stats");
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Slice f = streamCodec.toByteArray(fieldMap);
      bos.write((String.valueOf(containerIndex) + ":").getBytes());
      bos.write((String.valueOf(timestamp) + ":").getBytes());
      bos.write(f.buffer, f.offset, f.length);
      bos.write("\n".getBytes());
      queue.add(new WriteOperation(containersStorage, bos.toByteArray(), false));
    }
  }

  @Override
  public void recordOperators(List<OperatorInfo> operatorList, long timestamp) throws IOException
  {
    for (OperatorInfo operatorInfo : operatorList) {
      FSPartFileCollection operatorStorage;
      if (!logicalOperatorStorageMap.containsKey(operatorInfo.name)) {
        operatorStorage = new FSPartFileCollection();
        operatorStorage.setBasePath(basePath + "/operators/" + operatorInfo.name);
        operatorStorage.setup();
        operatorStorage.writeMetaData((VERSION + "\n").getBytes());
        logicalOperatorStorageMap.put(operatorInfo.name, operatorStorage);
      }
      else {
        operatorStorage = logicalOperatorStorageMap.get(operatorInfo.name);
      }
      if (!knownOperators.contains(operatorInfo.id)) {
        knownOperators.add(operatorInfo.id);
        Map<String, Object> fieldMap = extractRecordFields(operatorInfo, "meta");
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Slice f = streamCodec.toByteArray(fieldMap);
        bos.write(f.buffer, f.offset, f.length);
        bos.write("\n".getBytes());
        queue.add(new WriteOperation(operatorStorage, bos.toByteArray(), true));
      }
      Map<String, Object> fieldMap = extractRecordFields(operatorInfo, "stats");
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Slice f = streamCodec.toByteArray(fieldMap);
      bos.write((operatorInfo.id + ":").getBytes());
      bos.write((String.valueOf(timestamp) + ":").getBytes());
      bos.write(f.buffer, f.offset, f.length);
      bos.write("\n".getBytes());
      queue.add(new WriteOperation(operatorStorage, bos.toByteArray(), false));
    }
  }

  public Map<String, Object> extractRecordFields(Object o, String type)
  {
    Map<String, Object> fieldMap = new HashMap<String, Object>();
    try {
      Map<Class<?>, List<Field>> cacheFields = null;
      if (type.equals("meta")) {
        cacheFields = metaFields;
      }
      else if (type.equals("stats")) {
        cacheFields = statsFields;
      }
      List<Field> fieldList;
      if (cacheFields == null || !cacheFields.containsKey(o.getClass())) {
        fieldList = new ArrayList<Field>();
        for (Class<?> c = o.getClass(); c != Object.class; c = c.getSuperclass()) {
          Field[] fields = c.getDeclaredFields();
          for (Field field : fields) {
            field.setAccessible(true);
            RecordField rfa = field.getAnnotation(RecordField.class);
            if (rfa != null && rfa.type().equals(type)) {
              fieldList.add(field);
            }
          }
        }
        if (cacheFields != null) {
          cacheFields.put(o.getClass(), fieldList);
        }
      }
      else {
        fieldList = cacheFields.get(o.getClass());
      }

      for (Field field : fieldList) {
        fieldMap.put(field.getName(), field.get(o));
      }
    }
    catch (IllegalAccessException ex) {
      throw new RuntimeException(ex);
    }
    return fieldMap;
  }

  public void requestSync()
  {
    containersStorage.requestSync();
    for (Map.Entry<String, FSPartFileCollection> entry : logicalOperatorStorageMap.entrySet()) {
      entry.getValue().requestSync();
    }
  }

}
