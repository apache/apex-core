/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.api;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Operator;

import com.datatorrent.stram.StreamingContainerUmbilicalProtocol.StramToNodeRequest;
import com.datatorrent.stram.api.NodeRequest.RequestType;
import com.datatorrent.stram.engine.Node;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

/**
 * <p>RequestFactory class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.5
 */
public class RequestFactory
{
  private EnumMap<RequestType, RequestDelegate> map;

  public RequestFactory()
  {
    this.map = new EnumMap<RequestType, RequestDelegate>(RequestType.class);
  }

  public interface RequestDelegate
  {
    public NodeRequest getRequestExecutor(final Node<?> node, final StramToNodeRequest snr);

  }

  public void registerDelegate(NodeRequest.RequestType requestType, RequestDelegate delegate)
  {
    RequestDelegate old = map.put(requestType, delegate);
    if (old != null) {
      logger.warn("Replacing delegate {} for {} by {}", new Object[] {old, requestType, delegate});
    }
  }

  /**
   * Process request from stram for further communication through the protocol. Extended reporting is on a per node basis (won't occur under regular operation)
   *
   * @param node - Node which will be handling this request.
   * @param snr - The serialized request which contains context for the request.
   * @return - The actual object which will handle the request.
   */
  public NodeRequest getRequestExecutor(final Node<?> node, final StramToNodeRequest snr)
  {
    RequestDelegate delegate = map.get(snr.requestType);
    if (delegate == null) {
      /*
       * Fow now SET_PROPERTY is not delegated but as soon as its home is found, it should be sent there.
       * No special handling for any fucking thing!
       */
      if (snr.requestType == RequestType.SET_PROPERTY) {
        return new NodeRequest()
        {
          @Override
          public void execute(Operator operator, int id, long windowId) throws IOException
          {
            final Map<String, String> properties = Collections.singletonMap(snr.setPropertyKey, snr.setPropertyValue);
            logger.info("Setting property {} on operator {}", properties, operator);
            LogicalPlanConfiguration.setOperatorProperties(operator, properties);
          }

          @Override
          public String toString()
          {
            return "Set Property";
          }

        };
      }

      return null;
    }

    return delegate.getRequestExecutor(node, snr);
  }

  private static final Logger logger = LoggerFactory.getLogger(RequestFactory.class);
}
