/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.slf4j.impl;

import java.util.Map;

import junit.framework.Assert;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import com.datatorrent.stram.StreamingAppMaster;
import com.datatorrent.stram.api.StramEvent;
import com.datatorrent.stram.client.DTConfiguration;

public class DTLoggerFactoryTest
{

  @Test
  public void test()
  {
    String loggersLevel = "com.datatorrent.stram.client.*:INFO,com.datatorrent.stram.api.*:DEBUG";
    if (!Strings.isNullOrEmpty(loggersLevel)) {
      Map<String, String> targetChanges = Maps.newHashMap();
      String targets[] = loggersLevel.split(",");
      for (String target : targets) {
        String parts[] = target.split(":");
        targetChanges.put(parts[0], parts[1]);
      }
      DTLoggerFactory.get().changeLoggersLevel(targetChanges);
    }
    LoggerFactory.getLogger(DTConfiguration.class);
    LoggerFactory.getLogger(StramEvent.class);
    LoggerFactory.getLogger(StreamingAppMaster.class);

    org.apache.log4j.Logger dtConfigLogger = LogManager.getLogger(DTConfiguration.class);
    Assert.assertEquals(dtConfigLogger.getLevel(), Level.INFO);

    org.apache.log4j.Logger stramEventLogger = LogManager.getLogger(StramEvent.class);
    Assert.assertEquals(stramEventLogger.getLevel(), Level.DEBUG);

    org.apache.log4j.Logger streamingAppMasterLogger = LogManager.getLogger(StreamingAppMaster.class);
    Assert.assertNull(streamingAppMasterLogger.getLevel());
  }
}
