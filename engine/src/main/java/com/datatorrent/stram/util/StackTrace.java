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
package com.datatorrent.stram.util;

import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StackTrace
{
  private static final Logger LOG = LoggerFactory.getLogger(StackTrace.class);

  public static String getJsonFormat()
  {
    Map<Thread, StackTraceElement[]> stackTraces = Thread.getAllStackTraces();

    JSONObject jsonObject = new JSONObject();
    JSONArray jsonArray = new JSONArray();

    int j = 0;
    for (Map.Entry<Thread,StackTraceElement[]> elements : stackTraces.entrySet()) {

      JSONObject jsonThreads = new JSONObject();

      Thread thread = elements.getKey();

      try {

        jsonThreads.put("Name", thread.getName());
        jsonThreads.put("State", thread.getState());
        jsonThreads.put("ID", thread.getId());

        JSONArray stacks = new JSONArray();

        for (int i = 0; i < elements.getValue().length; ++i) {

          stacks.put(i, elements.getValue()[i].toString());
        }

        jsonThreads.put("Stacks", stacks);

        jsonArray.put(j++, jsonThreads);
      } catch (Exception ex) {
        LOG.warn("Getting stack trace for the thread " + thread.getName() + " failed.");
        continue;
      }
    }

    try {
      jsonObject.put("Threads", jsonArray);
    } catch (JSONException e) {
      LOG.error("Getting the stack trace failed with the exception " + e.toString());
    }

    return jsonObject.toString();
  }
}
