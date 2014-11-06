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
import com.datatorrent.api.StatsListener.OperatorCommand;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.StramToNodeRequest;
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
  private final EnumMap<StramToNodeRequest.RequestType, RequestDelegate> map;

  public RequestFactory()
  {
    this.map = new EnumMap<StramToNodeRequest.RequestType, RequestDelegate>(StramToNodeRequest.RequestType.class);
  }

  public interface RequestDelegate
  {
    public OperatorCommand getRequestExecutor(final Node<?> node, final StramToNodeRequest snr);

  }

  public void registerDelegate(StramToNodeRequest.RequestType requestType, RequestDelegate delegate)
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
  public OperatorCommand getRequestExecutor(final Node<?> node, final StramToNodeRequest snr)
  {
    RequestDelegate delegate = map.get(snr.requestType);
    if (delegate == null) {
      /*
       * For now SET_PROPERTY is not delegated but as soon as its home is found, it should be sent there.
       * No special handling for any fucking thing!
       */
      if (snr.cmd != null) {
        return snr.cmd;
      } else if (snr.requestType == StramToNodeRequest.RequestType.SET_PROPERTY) {
        return new OperatorCommand()
        {
          @Override
          public void execute(Operator operator, int id, long windowId) throws IOException
          {
            StramToNodeSetPropertyRequest snspr = (StramToNodeSetPropertyRequest)snr;
            final Map<String, String> properties = Collections.singletonMap(snspr.getPropertyKey(), snspr.getPropertyValue());
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
