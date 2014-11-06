/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.api;

import java.util.Set;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.*;

import com.datatorrent.api.Attribute;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
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
  };

  interface iface
  {
    Attribute<Greeting> greeting = new Attribute<Greeting>(Greeting.hello);
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