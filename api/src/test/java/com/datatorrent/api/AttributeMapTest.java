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

import com.datatorrent.api.AttributeMap.Attribute;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class AttributeMapTest
{
  @Test
  public void testGetAttributes()
  {
    assertTrue("Identity of Interface", DAGContext.serialVersionUID != 0);
    Set<Attribute<Object>> result = com.datatorrent.api.AttributeMap.AttributeInitializer.getAttributes(DAGContext.class);
    assertTrue("Attributes Collection", !result.isEmpty());
    for (Attribute<Object> attribute : result) {
      logger.debug("{}", attribute);
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(AttributeMapTest.class);
}