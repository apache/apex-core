/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api;

import io.netty.util.AttributeKey;
import io.netty.util.AttributeMap;

/**
 *
 * The base interface for context for all of the streaming platform objects<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface Context extends AttributeMap
{
  public static final AttributeKey<Integer> INPUT_PORT_BUFFER_SIZE = new AttributeKey<Integer>("INPUT_PORT_BUFFER_SIZE");
  public static final AttributeKey<Integer> OPERATOR_SPIN_MILLIS = new AttributeKey<Integer>("OPERATOR_SPIN_MILLIS");
}
