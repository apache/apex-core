/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.webapp;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
@Path(WebServices.PATH)
public class WebServices
{
  public static final String VERSION = "v1";
  public static final String PATH = "/ws";

  @GET
  public String getVersion() {
    return "{\"version\": \"" + VERSION + "\"}";
  }
}
