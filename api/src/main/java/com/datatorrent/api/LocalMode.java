package com.datatorrent.api;

import java.util.Iterator;
import java.util.ServiceLoader;

import org.apache.hadoop.conf.Configuration;

/**
 * Local mode execution for single application
 */
public abstract class LocalMode {

  abstract public DAG getDAG();
  abstract public DAG cloneDAG() throws Exception;
  abstract public Controller getController();

  public interface Controller {
    public void run();
    public void run(long runMillis);
    public void runAsync();
    public void shutdown();
    public void setHeartbeatMonitoringEnabled(boolean enabled);
  }

  public static LocalMode newInstance() {
    ServiceLoader<LocalMode> loader = ServiceLoader.load(LocalMode.class);
    Iterator<LocalMode> impl = loader.iterator();
    if (!impl.hasNext()) {
      throw new RuntimeException("No implementation for " + LocalMode.class);
    }
    return impl.next();
  }

  /**
   * Shortcut to run an application. Used for testing.
   * @param app
   * @param runMillis
   */
  public static void runApp(StreamingApplication app, int runMillis) {
    LocalMode lma = newInstance();
    app.populateDAG(lma.getDAG(), new Configuration(false));
    LocalMode.Controller lc = lma.getController();
    lc.run(runMillis);
  }

}
