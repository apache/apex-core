/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.AbstractNode;
import com.malhartech.dag.NodeConfiguration;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;

/**
 *
 * Sends tuple as POST with JSON content to the given URL<p>
 * <br>
 * Maps are converted to JSON. All other tuple types are sent in their toString representation.<br>
 * <br>
 *
 */
@NodeAnnotation(
    ports = {
        @PortAnnotation(name = "input", type = PortType.INPUT)
    }
)
public class HttpOutputNode extends AbstractNode
{
  private static final Logger LOG = LoggerFactory.getLogger(HttpOutputNode.class);

  /**
   * The URL of the web service resource for the POST request.
   */
  public static final String P_RESOURCE_URL = "resourceUrl";

  private transient URI resourceUrl;
  private transient Client wsClient;
  private transient WebResource resource;

  @Override
  public void setup(NodeConfiguration config)  {
    super.setup(config);
    checkConfiguration(config);

    wsClient = Client.create();
    wsClient.setFollowRedirects(true);
    resource = wsClient.resource(resourceUrl);
  }

  @Override
  public void teardown() {
    if (wsClient != null) {
      wsClient.destroy();
    }
    super.teardown();
  }

  @Override
  public boolean checkConfiguration(NodeConfiguration config) {
    String urlStr = config.get(P_RESOURCE_URL);
    if (urlStr == null) {
      throw new IllegalArgumentException(String.format("Missing '%s' in configuration.", P_RESOURCE_URL));
    }
    try {
      this.resourceUrl = new URI(urlStr);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(String.format("Invalid value '%s' for '%s'.", urlStr, P_RESOURCE_URL));
    }
    return true;
  }

  /**
   *
   * @param t the value of t
   */
  @Override
  public void process(Object t)
  {
    try {
      if (t instanceof Map) {
        resource.type(MediaType.APPLICATION_JSON).post(new JSONObject((Map<?,?>)t));
      } else {
        resource.post(""+t);
      }
    }
    catch (Exception e) {
      LOG.error("Failed to send tuple to " + resource.getURI());
      //throw new IllegalArgumentException("Failed to send tuple to " + r.getURI(), e);
    }
  }

}
