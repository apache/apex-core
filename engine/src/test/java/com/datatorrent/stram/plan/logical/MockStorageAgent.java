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
package com.datatorrent.stram.plan.logical;

import java.io.IOException;

import com.datatorrent.api.StorageAgent;

public class MockStorageAgent implements StorageAgent
{
  public MockStorageAgent()
  {
  }

  @Override
  public void save(Object object, int operatorId, long windowId) throws IOException
  {
    // Do nothing for now
    return;
  }

  @Override
  public Object load(int operatorId, long windowId) throws IOException
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void delete(int operatorId, long windowId) throws IOException
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public long[] getWindowIds(int operatorId) throws IOException
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String toString()
  {
    return MockStorageAgent.class.getCanonicalName();
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null) {
      return false;
    }

    return obj instanceof MockStorageAgent;
  }
}
