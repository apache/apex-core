/*
 *  Copyright (c) 2012-2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.api;

import java.io.IOException;
import org.codehaus.jettison.json.JSONObject;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public interface AppDataPusher
{
  public void push(JSONObject appData) throws IOException;

  public long getResendSchemaInterval();
}
