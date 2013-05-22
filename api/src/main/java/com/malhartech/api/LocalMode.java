package com.malhartech.api;

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
    String className = "com.malhartech.stram.LocalModeAppImpl";
    try {
      Class<? extends LocalMode> clazz = Thread.currentThread().getContextClassLoader().loadClass(className).asSubclass(LocalMode.class);
      try {
        return clazz.newInstance();
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException("Failed to instantiate " + clazz, e);
      } catch (InstantiationException e) {
        throw new IllegalArgumentException("Failed to instantiate " + clazz, e);
      }
    }
    catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Class not found: " + className, e);
    }
  }

  /**
   * Shortcut to run an application. Used for testing.
   * @param app
   * @param runMillis
   */
  public static void runApp(ApplicationFactory app, int runMillis) {
    LocalMode lma = newInstance();
    app.getApplication(lma.getDAG(), new Configuration(false));
    LocalMode.Controller lc = lma.getController();
    lc.run(runMillis);
  }

}
