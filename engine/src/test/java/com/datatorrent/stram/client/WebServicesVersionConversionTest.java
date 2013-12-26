/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.client;

import com.datatorrent.stram.client.WebServicesVersionConversion.VersionConversionFilter;
import com.datatorrent.stram.util.WebServicesClient;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;

/**
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.9.2
 */
public class WebServicesVersionConversionTest
{
  private int port = 12441;
  private Server server;

  class DummyVersionHandler extends AbstractHandler
  {
    @Override
    public void handle(String target, HttpServletRequest request, HttpServletResponse response, int i) throws IOException, ServletException
    {
      System.out.println("Target: " + target);
      System.out.println("Request URL: " + request.getRequestURI());
      response.setContentType(MediaType.APPLICATION_JSON);
      JSONObject json = new JSONObject();
      try {
        json.put("old_key", "value");
        json.put("other_key", "other_value");
        json.put("url", request.getRequestURI());
      }
      catch (JSONException ex) {
      }
      response.getWriter().println(json.toString());
      response.setStatus(200);
      ((Request)request).setHandled(true);
    }

  }

  @Before
  public void setup() throws Exception
  {
    server = new Server(port);
    server.setHandler(new DummyVersionHandler());
    server.start();
  }

  @After
  public void teardown() throws Exception
  {
    server.stop();
    server.join();
  }

  @Test
  public void testVersioning() throws Exception
  {
    WebServicesClient wsClient = new WebServicesClient();
    Client client = wsClient.getClient();

    WebResource ws = client.resource("http://localhost:" + port).path("/new_path");
    WebServicesVersionConversion.Converter versionConverter = new WebServicesVersionConversion.Converter()
    {
      @Override
      public String convertCommandPath(String path)
      {
        if (path.equals("/new_path")) {
          return "/old_path";
        }
        return path;
      }

      @Override
      public String convertResponse(String path, String response)
      {
        if (path.equals("/new_path")) {
          try {
            JSONObject json = new JSONObject(response);
            json.put("new_key", json.get("old_key"));
            json.remove("old_key");
            return json.toString();
          }
          catch (JSONException ex) {
            throw new RuntimeException(ex);
          }
        }
        return response;
      }

    };
    VersionConversionFilter versionConversionFilter = new VersionConversionFilter(versionConverter);
    client.addFilter(versionConversionFilter);
    JSONObject result = new JSONObject(ws.get(String.class));

    Assert.assertEquals(result.getString("url"), "/old_path");
    Assert.assertEquals(result.getString("new_key"), "value");
    Assert.assertEquals(result.getString("other_key"), "other_value");

  }

}
