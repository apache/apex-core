/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.bufferserver.packet;

import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;


/**
 *
 */
public class NoMessageTupleTest extends TestCase
{
  public NoMessageTupleTest(String testName)
  {
    super(testName);
  }

  @Override
  protected void setUp() throws Exception
  {
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception
  {
    super.tearDown();
  }

  @Test
  public void testSerDe()
  {
    logger.info("testSerDe");

    byte[] serialized = NoMessageTuple.getSerializedTuple();
    Tuple t = Tuple.getTuple(serialized, 0, serialized.length);

    assert(t.getType() == MessageType.NO_MESSAGE);
  }

  private static final Logger logger = LoggerFactory.getLogger(NoMessageTupleTest.class);
}
