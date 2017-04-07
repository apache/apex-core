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
package com.datatorrent.api;

import java.util.Set;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class AttributeMapTest
{
  @Test
  public void testGetAttributes()
  {
    assertTrue("Identity of Interface", com.datatorrent.api.Context.DAGContext.serialVersionUID != 0);
    Set<Attribute<Object>> result = com.datatorrent.api.Attribute.AttributeMap.AttributeInitializer.getAttributes(com.datatorrent.api.Context.DAGContext.class);
    assertTrue("Attributes Collection", !result.isEmpty());
    for (Attribute<Object> attribute : result) {
      logger.debug("{}", attribute);
    }
  }

  enum Greeting
  {
    hello,
    howdy
  }

  interface iface
  {
    Attribute<Greeting> greeting = new Attribute<>(Greeting.hello);
  }

  @Test
  public void testEnumAutoCodec()
  {
    com.datatorrent.api.Attribute.AttributeMap.AttributeInitializer.initialize(iface.class);
    Greeting howdy = iface.greeting.codec.fromString(Greeting.howdy.name());
    assertSame("Attribute", Greeting.howdy, howdy);
  }

  private static final Logger logger = LoggerFactory.getLogger(AttributeMapTest.class);
}
