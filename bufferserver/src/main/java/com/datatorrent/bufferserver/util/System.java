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
package com.datatorrent.bufferserver.util;

import java.io.IOException;
import java.util.HashMap;

import com.datatorrent.netlet.DefaultEventLoop;
import com.datatorrent.netlet.EventLoop;

/**
 * <p>System class.</p>
 *
 * @since 0.3.2
 */
public class System
{
  private static final HashMap<String, DefaultEventLoop> eventloops = new HashMap<>();

  public static void startup(String identifier)
  {
    synchronized (eventloops) {
      DefaultEventLoop el = eventloops.get(identifier);
      if (el == null) {
        try {
          eventloops.put(identifier, el = DefaultEventLoop.createEventLoop(identifier));
        } catch (IOException io) {
          throw new RuntimeException(io);
        }
      }
      el.start();
    }
  }

  public static void shutdown(String identifier)
  {
    synchronized (eventloops) {
      DefaultEventLoop el = eventloops.get(identifier);
      if (el == null) {
        throw new RuntimeException("System with " + identifier + " not setup!");
      } else {
        el.stop();
      }
    }
  }

  public static EventLoop getEventLoop(String identifier)
  {
    synchronized (eventloops) {
      return eventloops.get(identifier);
    }
  }

}
