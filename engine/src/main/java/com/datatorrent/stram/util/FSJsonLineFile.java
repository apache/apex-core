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

import java.io.Closeable;
import java.io.IOException;
import java.util.EnumSet;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * <p>FSJsonLineFile class.</p>
 *
 * @since 1.0.2
 */
public class FSJsonLineFile implements Closeable
{
  private final ObjectMapper objectMapper;
  private final FSDataOutputStream os;
  private static final Logger LOG = LoggerFactory.getLogger(FSJsonLineFile.class);

  public FSJsonLineFile(FileContext fileContext, Path path, FsPermission permission) throws IOException
  {
    this.os = fileContext.create(path, EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND), Options.CreateOpts.perms(permission));
    this.objectMapper = (new JSONSerializationProvider()).getContext(null);
  }

  public synchronized void append(JSONObject json) throws IOException
  {
    os.writeBytes(json.toString() + "\n");
    os.hflush();
  }

  public synchronized void append(Object obj) throws IOException
  {
    os.writeBytes(objectMapper.writeValueAsString(obj) + "\n");
    os.hflush();
  }

  @Override
  public void close() throws IOException
  {
    os.close();
  }

}
