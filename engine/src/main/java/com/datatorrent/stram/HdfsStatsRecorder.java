/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import com.datatorrent.stram.util.HdfsPartFileCollection;
import com.datatorrent.stram.webapp.ContainerInfo;
import com.datatorrent.stram.webapp.OperatorInfo;
import com.datatorrent.api.codec.JsonStreamCodec;
import com.datatorrent.api.annotation.RecordField;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.common.util.Slice;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class HdfsStatsRecorder
{
  public static final String VERSION = "1.0";
  private String basePath = ".";
  private HdfsPartFileCollection containersStorage;
  private Map<String, HdfsPartFileCollection> logicalOperatorStorageMap = new HashMap<String, HdfsPartFileCollection>();
  private Map<String, Integer> knownContainers = new HashMap<String, Integer>();
  private Set<String> knownOperators = new HashSet<String>();
  private transient StreamCodec<Object> streamCodec;
  private Map<Class<?>, List<Field>> metaFields = new HashMap<Class<?>, List<Field>>();
  private Map<Class<?>, List<Field>> statsFields = new HashMap<Class<?>, List<Field>>();

  public void setBasePath(String basePath)
  {
    this.basePath = basePath;
  }

  public void setup()
  {
    try {
      streamCodec = new JsonStreamCodec<Object>();
      containersStorage = new HdfsPartFileCollection();
      containersStorage.setBasePath(basePath + "/containers");
      containersStorage.setup();
      containersStorage.writeMetaData((VERSION + "\n").getBytes());
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public void recordContainers(Map<String, StramChildAgent> containerMap, long timestamp) throws IOException
  {
    for (Map.Entry<String, StramChildAgent> entry : containerMap.entrySet()) {
      StramChildAgent sca = entry.getValue();
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
        containersStorage.writeMetaData(bos.toByteArray());
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
      containersStorage.writeDataItem(bos.toByteArray(), true);
      containersStorage.checkTurnover();
    }
  }

  public void recordOperators(List<OperatorInfo> operatorList, long timestamp) throws IOException
  {
    for (OperatorInfo operatorInfo : operatorList) {
      HdfsPartFileCollection operatorStorage;
      if (!logicalOperatorStorageMap.containsKey(operatorInfo.name)) {
        operatorStorage = new HdfsPartFileCollection();
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
        operatorStorage.writeMetaData(bos.toByteArray());
      }
      Map<String, Object> fieldMap = extractRecordFields(operatorInfo, "stats");
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Slice f = streamCodec.toByteArray(fieldMap);
      bos.write((operatorInfo.id + ":").getBytes());
      bos.write((String.valueOf(timestamp) + ":").getBytes());
      bos.write(f.buffer, f.offset, f.length);
      bos.write("\n".getBytes());
      operatorStorage.writeDataItem(bos.toByteArray(), true);
    }
    for (HdfsPartFileCollection operatorStorage : logicalOperatorStorageMap.values()) {
      operatorStorage.checkTurnover();
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
          for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
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
    for (Map.Entry<String, HdfsPartFileCollection> entry : logicalOperatorStorageMap.entrySet()) {
      entry.getValue().requestSync();
    }
  }

}
