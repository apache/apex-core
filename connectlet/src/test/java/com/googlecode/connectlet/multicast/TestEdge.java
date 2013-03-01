package com.googlecode.connectlet.multicast;

import java.io.FileInputStream;
import java.util.Properties;

import com.googlecode.connectlet.Connector;
import com.googlecode.connectlet.misc.DumpFilterFactory;
import com.googlecode.connectlet.util.ClassPath;

public class TestEdge
{
  private static Connector newConnector()
  {
    return new Connector();
  }

  public static void main(String[] args) throws Exception
  {
    Properties p = new Properties();
    try {
      FileInputStream in = new FileInputStream(ClassPath.
              getInstance("../conf/EdgeConnectlet.properties"));
      p.load(in);
      in.close();
    }
    finally {
    }
    int clientPort = Integer.parseInt(p.getProperty("client_port"));
    String originHost = p.getProperty("origin_host");
    int originPort = Integer.parseInt(p.getProperty("origin_port"));
    boolean dump = Boolean.parseBoolean(p.getProperty("dump"));
    // Evade resource leak warning
    Connector connector = newConnector();
    EdgeServer edge = new EdgeServer(clientPort);
    connector.add(edge);
    connector.connect(edge.getOriginConnection(), originHost, originPort);
    if (dump) {
      edge.getFilterFactories().add(new DumpFilterFactory());
    }
    while (edge.getOriginConnection().isOpen()) {
      while (connector.doEvents()) {/**/

      }
      Thread.sleep(16);
    }
  }

}