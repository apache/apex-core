/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api;

import com.malhartech.util.ContextAttributes;

/**
 *
 * The base interface for context for all of the streaming platform objects<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface Context
{
  public interface PortContext extends Context {
    public static final AttributeKey<Integer> INPUT_PORT_BUFFER_SIZE = new AttributeKey<Integer>("INPUT_PORT_BUFFER_SIZE");

    public class AttributeKey<T> extends ContextAttributes.AttributeKey<PortContext, T> {
      private AttributeKey(String name) {
        super(PortContext.class, name);
      }
    }

    ContextAttributes.AttributeMap<PortContext> getAttributes();

  }

  public interface OperatorContext extends Context {
    public static final AttributeKey<Integer> SPIN_MILLIS = new AttributeKey<Integer>("SPIN_MILLIS");

    public class AttributeKey<T> extends ContextAttributes.AttributeKey<OperatorContext, T> {
      private AttributeKey(String name) {
        super(OperatorContext.class, name);
      }
    }

    /**
     * Return the operator runtime id.
     * @return
     */
    String getId();

    ContextAttributes.AttributeMap<OperatorContext> getAttributes();

  }

}
