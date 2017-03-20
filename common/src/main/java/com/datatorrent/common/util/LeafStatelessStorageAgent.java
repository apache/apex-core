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
package com.datatorrent.common.util;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.util.EnumSet;

import org.slf4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;

import com.datatorrent.api.annotation.Stateless;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * A StorageAgent which write empty checkpoint files for an operator. This StorageAgent is used
 * by stateless leaf operators to make sure that recovery works correctly after application
 * restart. see APEXCORE-619.
 */
public class LeafStatelessStorageAgent extends FSStorageAgent
{
  private static final Logger LOG = getLogger(LeafStatelessStorageAgent.class);

  public LeafStatelessStorageAgent(String path, Configuration conf)
  {
    super(path, conf);
  }

  /**
   * Allow writing initial checkpoint, after that for any other windowId this agent creates a
   * zero length file.
   *
   * @param object
   * @param operatorId
   * @param windowId
   * @throws IOException
   */
  @Override
  public void save(Object object, int operatorId, long windowId) throws IOException
  {
    if (windowId == Stateless.WINDOW_ID) {
      super.save(object, operatorId, windowId);
    } else {
      FSDataOutputStream fout = null;
      try {
        fout = fileContext.create(getCheckpointPath(operatorId, windowId), EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE));
      } finally {
        if (fout != null) {
          fout.close();
        }
      }
    }
  }

  /**
   * Always load initial checkpoint.
   *
   * @param operatorId
   * @param windowId
   * @return
   * @throws IOException
   */
  @Override
  public Object load(int operatorId, long windowId) throws IOException
  {
    return super.load(operatorId, Stateless.WINDOW_ID);
  }

  @Override
  public Object readResolve() throws ObjectStreamException
  {
    LeafStatelessStorageAgent emptyFSStorageAgent = new LeafStatelessStorageAgent(this.path, null);
    return emptyFSStorageAgent;
  }
}
