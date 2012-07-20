/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stram;

import com.malhartech.dag.*;
import com.malhartech.stram.conf.TopologyBuilder;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper node to connects adapter "stream" to another source or sink Provides
 * uniform view of adapter as node for stram deployment and monitoring.
 */
public class AdapterWrapperNode extends AbstractNode implements Sink
{
  private final static Logger logger = LoggerFactory.getLogger(AdapterWrapperNode.class);
  public static final String KEY_STREAM_CLASS_NAME = "streamClassName";
  public static final String KEY_IS_INPUT = "input";
  private String streamClassName;
  private Stream adapterStream = null;

  public AdapterWrapperNode(NodeContext ctx)
  {
    super(ctx);
  }

  public String getStreamClassName()
  {
    return streamClassName;
  }

  public void setStreamClassName(String streamClassName)
  {
    this.streamClassName = streamClassName;
  }

  public boolean isInput()
  {
    return adapterStream instanceof InputAdapter;
  }

  public InputAdapter getInputAdapter()
  {
    return (InputAdapter) adapterStream;
  }

  @Override
  public void process(Object payload)
  {
    throw new UnsupportedOperationException("Adapter nodes do not implement process.");
  }

  @Override
  public void doSomething(Tuple t)
  {
    sink.sink(t);
  }

  @Override
  public void setup(NodeConfiguration config)
  {
    Map<String, String> props = config.getDagProperties();
    props.put(TopologyBuilder.STREAM_CLASSNAME, this.streamClassName);
    StreamConfiguration streamConf = new StreamConfiguration(props);
    if (isInput()) {
      InputAdapter inputAdapter = initAdapterStream(streamConf, this);
      adapterStream = inputAdapter;
    }
    else {
      adapterStream = initAdapterStream(streamConf, null);
    }
  }

  @Override
  public void teardown()
  {
    if (adapterStream != null) {
      adapterStream.teardown();
    }
  }
  
  
  private com.malhartech.dag.StreamContext sink;

  public void setOutputStream(com.malhartech.dag.StreamContext context)
  {
    sink = context;
  }

  @Override
  public Sink getSink(com.malhartech.dag.StreamContext context)
  {
    if (isInput()) {
      return this;
    }
    else {
      // output adapter
      return super.getSink(context);
    }
  }

  public static <T extends Stream> T initAdapterStream(StreamConfiguration streamConf, AbstractNode node)
  {
    Map<String, String> properties = streamConf.getDagProperties();
    String className = properties.get(TopologyBuilder.STREAM_CLASSNAME);
    if (className == null) {
      // should have been caught during submit validation
      throw new IllegalArgumentException(String.format("Stream class not configured (key '%s')", TopologyBuilder.STREAM_CLASSNAME));
    }
    try {
      Class<?> clazz = Class.forName(className);
      Class<? extends Stream> subClass = clazz.asSubclass(Stream.class);
      Constructor<? extends Stream> c = subClass.getConstructor();
      @SuppressWarnings("unchecked")
      T instance = (T) c.newInstance();
      // populate custom properties
      BeanUtils.populate(instance, properties);

      instance.setup(streamConf);

      com.malhartech.dag.StreamContext ctx = new com.malhartech.dag.StreamContext();
      if (node == null) {
        /*
         * This is output adapter so it needs to implement the Sink interface.
         */
        if (instance instanceof Sink) {
          logger.info(ctx + " setting selfsink for instance " + instance);
          ctx.setSink((Sink) instance);
        }
      }
      else {
        Sink sink = node.getSink(ctx);
        logger.info(ctx + " setting sink for instance " + instance + " to " + sink);
        ctx.setSink(sink);
      }

      ctx.setSerde(StramUtils.getSerdeInstance(properties));
      instance.setContext(ctx);

      return instance;
    }
    catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Node class not found: " + className, e);
    }
    catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Error setting node properties", e);
    }
    catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Error setting node properties", e);
    }
    catch (SecurityException e) {
      throw new IllegalArgumentException("Error creating instance of class: " + className, e);
    }
    catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Constructor with NodeContext not found: " + className, e);
    }
    catch (InstantiationException e) {
      throw new IllegalArgumentException("Failed to instantiate: " + className, e);
    }
  }

  @Override
  public void handleIdleTimeout()
  {
    logger.info("adapter timed out");
    if (isInput() && ((InputAdapter) adapterStream).hasFinished()) {
      logger.info("it was input adapter... stopping now");
      stopSafely();
    }
  }
}
