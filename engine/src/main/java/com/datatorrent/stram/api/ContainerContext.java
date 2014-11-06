/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.api;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Attribute.AttributeMap.AttributeInitializer;
import com.datatorrent.api.Context;

/**
 * <p>ContainerContext interface.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.5
 */
public interface ContainerContext extends Context
{
  public static final Attribute<String> IDENTIFIER = new Attribute<String>("unknown_container_id");
  public static final Attribute<RequestFactory> REQUEST_FACTORY = new Attribute<RequestFactory>(null, null);
  @SuppressWarnings("FieldNameHidesFieldInSuperclass")
  long serialVersionUID = AttributeInitializer.initialize(ContainerContext.class);
}
