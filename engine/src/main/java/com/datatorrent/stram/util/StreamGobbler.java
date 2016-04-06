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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>StreamGobbler class.</p>
 *
 * @since 0.9.4
 */
public class StreamGobbler extends Thread
{
  InputStream is;
  StringBuilder content = new StringBuilder();
  private static final Logger LOG = LoggerFactory.getLogger(StreamGobbler.class);

  public StreamGobbler(InputStream is)
  {
    this.is = is;
  }

  public String getContent()
  {
    try {
      return content.toString();
    } finally {
      content.setLength(0);
    }
  }

  @Override
  public void run()
  {
    try {
      InputStreamReader isr = new InputStreamReader(is);
      BufferedReader br = new BufferedReader(isr);
      String line;
      try {
        while ((line = br.readLine()) != null) {
          if (!line.contains(" DEBUG ")) {
            content.append(line);
            content.append("\n");
          }
        }
      } finally {
        br.close();
      }
    } catch (IOException ex) {
      LOG.error("Caught exception", ex);
    }
  }

}
