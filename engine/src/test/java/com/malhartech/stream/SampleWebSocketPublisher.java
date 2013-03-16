/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import javax.ws.rs.core.MediaType;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class SampleWebSocketPublisher implements Runnable
{
  private String channelUrl = "http://localhost:9090/channel/testChannel";

  public void setChannelUrl(String channelUrl)
  {
    this.channelUrl = channelUrl;
  }

  @Override
  public void run()
  {
    try {
      String payload = "{\"hello\":\"world\"}";
      URL url = new URL(channelUrl);
      while (true) {
        HttpURLConnection connection = (HttpURLConnection)url.openConnection();
        connection.setDoOutput(true);
        connection.setDoInput(true);
        connection.setInstanceFollowRedirects(false);
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", MediaType.APPLICATION_JSON);
        connection.setRequestProperty("charset", "utf-8");
        connection.setRequestProperty("Content-Length", "" + Integer.toString(payload.getBytes().length));
        connection.setUseCaches(false);


        DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
        wr.writeBytes(payload);
        wr.flush();
        wr.close();
        InputStream is = connection.getInputStream();
        byte[] buf = new byte[4096];
        while (is.read(buf) > 0) {
        }
        Thread.sleep(1000);
        connection.disconnect();
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static void main(String[] args) throws Exception
  {
    SampleWebSocketPublisher sp = new SampleWebSocketPublisher();
    sp.run();
  }

}
