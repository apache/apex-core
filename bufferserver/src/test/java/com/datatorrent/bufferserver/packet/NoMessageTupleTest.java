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
package com.datatorrent.bufferserver.packet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.testng.annotations.Test;


/**
 *
 */
public class NoMessageTupleTest
{
  @Test
  public void testSerDe()
  {
    logger.info("testSerDe");

    byte[] serialized = NoMessageTuple.getSerializedTuple();
    Tuple t = Tuple.getTuple(serialized, 0, serialized.length);

    assert t.getType() == MessageType.NO_MESSAGE;
  }

  private static final Logger logger = LoggerFactory.getLogger(NoMessageTupleTest.class);
}
