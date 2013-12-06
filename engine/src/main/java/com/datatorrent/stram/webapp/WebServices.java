/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.webapp;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * <p>WebServices class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.9.0
 */
@Path(WebServices.PATH)
public class WebServices
{
  public static final String VERSION = "v1";
  public static final String PATH = "/ws";

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public JSONObject getVersion() throws JSONException
  {
    return new JSONObject("{\"version\": \"" + VERSION + "\"}");
  }
}
