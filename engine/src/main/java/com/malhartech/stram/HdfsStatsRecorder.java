/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.malhartech.annotation.RecordField;
import com.malhartech.api.StreamCodec;
import com.malhartech.stram.webapp.ContainerInfo;
import com.malhartech.util.Fragment;
import com.malhartech.util.HdfsPartFileCollection;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class HdfsStatsRecorder
{
  public static final String VERSION = "1.0";
  private String basePath = ".";
  private HdfsPartFileCollection containersStorage;
  private Map<String, HdfsPartFileCollection> logicalOperatorStorage = new HashMap<String, HdfsPartFileCollection>();
  private Map<String, Integer> knownContainers = new HashMap<String, Integer>();
  @SuppressWarnings("rawtypes")
  private Class<? extends StreamCodec> streamCodecClass = JsonStreamCodec.class;
  private transient StreamCodec<Object> streamCodec;

  public void setBasePath(String basePath)
  {
    this.basePath = basePath;
  }

  public void setup()
  {
    try {
      streamCodec = streamCodecClass.newInstance();
      containersStorage = new HdfsPartFileCollection();
      containersStorage.setBasePath(basePath + "/containers");
      containersStorage.setup();
      containersStorage.writeMetaData((VERSION + "\n").getBytes());
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public void recordContainers(Map<String, StramChildAgent> containerMap)
  {
    try {
      for (Map.Entry<String, StramChildAgent> entry: containerMap.entrySet()) {
        StramChildAgent sca = entry.getValue();
        ContainerInfo containerInfo = sca.getContainerInfo();
        if (!containerInfo.state.equals("ACTIVE")) {
          continue;
        }
        int containerIndex = knownContainers.size();
        if (!knownContainers.containsKey(entry.getKey())) {
          containerIndex = knownContainers.size();
          knownContainers.put(entry.getKey(), containerIndex);
          Map<String, Object> fieldMap = extractRecordFields(containerInfo, "meta");
          ByteArrayOutputStream bos = new ByteArrayOutputStream();
          Fragment f = streamCodec.toByteArray(fieldMap).data;
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
        Fragment f = streamCodec.toByteArray(fieldMap).data;
        bos.write((String.valueOf(containerIndex) + ":").getBytes());
        bos.write(f.buffer, f.offset, f.length);
        bos.write("\n".getBytes());
        containersStorage.writeDataItem(bos.toByteArray(), true);
        containersStorage.checkTurnover();
      }
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public void recordOperators()
  {
  }
  
  public static Map<String, Object> extractRecordFields(Object o, String type)
  {
    Map<String, Object> fieldMap = new HashMap<String, Object>();
    try {
      for (Class<?> c = o.getClass(); c != Object.class; c = c.getSuperclass()) {
        Field[] fields = c.getDeclaredFields();
        for (int i = 0; i < fields.length; i++) {
          Field field = fields[i];
          field.setAccessible(true);
          RecordField rfa = field.getAnnotation(RecordField.class);
          if (rfa != null && rfa.type().equals(type)) {
            fieldMap.put(field.getName(), field.get(o));
          }
        }
      }
    }
    catch (IllegalAccessException ex) {
      throw new RuntimeException(ex);
    }
    return fieldMap;
  }

}
