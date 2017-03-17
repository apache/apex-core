/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.api;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.ServiceLoader;

import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Throwables;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.StreamingApplication;

/**
 * A class that provides an entry point for functionality to run applications in different environments such as current
 * Java VM, Hadoop YARN etc.
 *
 * @since 3.5.0
 */
public abstract class Launcher<H extends Launcher.AppHandle>
{

  public static final String NEW_INSTANCE_METHOD = "newInstance";

  /**
   * Denotes an environment in which to launch the application. Also, contains the list of supported environments.
   * @param <L> The launcher for the specific environment
   */
  public static class LaunchMode<L extends Launcher>
  {
    /**
     * Launch application in the current Java VM
     */
    public static final LaunchMode<EmbeddedAppLauncher> EMBEDDED = new LaunchMode<>(EmbeddedAppLauncher.class);
    /**
     * Launch application on Hadoop YARN
     */
    public static final LaunchMode<YarnAppLauncher> YARN = new LaunchMode<>(YarnAppLauncher.class);

    Class<L> clazz;

    public LaunchMode(Class<L> clazz)
    {
      this.clazz = clazz;
    }
  }

  /**
   * Specifies the manner in which a running application be stopped.
   */
  public enum ShutdownMode
  {
    /**
     * Shutdown the application in an orderly fashion and wait till it stops running
     */
    AWAIT_TERMINATION,
    /**
     * Kill the application immediately
     */
    KILL
  }

  /**
   * Results of application launch. The client can interact with the running application through this handle.
   */
  public interface AppHandle
  {
    boolean isFinished();

    /**
     * Shutdown the application.
     *
     * The method takes the application handle and a shutdown mode. The shutdown mode specifies how to shutdown the
     * application.
     *
     * If the mode is AWAIT_TERMINATION, an attempt should be made to shutdown the application in an orderly fashion
     * and wait till termination. If the application does not terminate in a reasonable amount of time the
     * implementation can forcibly terminate the application.
     *
     * If the mode is KILL, the application can be killed immediately.
     *
     * @param shutdownMode The shutdown mode
     */
    void shutdown(ShutdownMode shutdownMode) throws LauncherException;

  }

  /**
   * Get a launcher instance.<br><br>
   *
   * Returns a launcher specific to the given launch mode. This allows the user to also use custom methods supported by
   * the specific launcher along with the basic launch methods from this class.
   *
   * @param launchMode - The launch mode to use
   *
   * @return The launcher
   */
  public static <L extends Launcher<?>> L getLauncher(LaunchMode<L> launchMode)
  {
    L launcher;
    // If the static method for creating a new instance is present in the launcher, it is invoked to create an instance.
    // This gives an opportunity for the launcher to do something custom when creating an instance. If the method is not
    // present, the service is loaded from the class name. A factory approach would be cleaner and type safe but adds
    // unnecessary complexity, going with the static method for now.
    try {
      Method m = launchMode.clazz.getDeclaredMethod(NEW_INSTANCE_METHOD);
      launcher = (L)m.invoke(null);
    } catch (NoSuchMethodException e) {
      launcher = loadService(launchMode.clazz);
    } catch (InvocationTargetException | IllegalAccessException e) {
      throw Throwables.propagate(e);
    }
    return launcher;
  }

  /**
   * Launch application with configuration.<br><br>
   *
   * Launch the given streaming application with the given configuration.
   *
   * @param application  - Application to be run
   * @param configuration - Application Configuration
   *
   * @return The application handle
   */
  public H launchApp(StreamingApplication application, Configuration configuration) throws LauncherException
  {
    return launchApp(application, configuration, null);
  }

  /**
   * Launch application with configuration and launch parameters.
   *
   * Launch the given streaming application with the given configuration and parameters. The parameters should be from
   * the list of parameters supported by the launcher. To find out more about the supported parameters look at the
   * documentation of the individual launcher.
   *
   * @param application  - Application to be run
   * @param configuration - Application Configuration
   * @param launchParameters - Launch Parameters
   *
   * @return The application handle
   */
  public abstract H launchApp(StreamingApplication application, Configuration configuration, Attribute.AttributeMap launchParameters) throws LauncherException;

  protected static <T> T loadService(Class<T> clazz)
  {
    ServiceLoader<T> loader = ServiceLoader.load(clazz);
    Iterator<T> impl = loader.iterator();
    if (!impl.hasNext()) {
      throw new RuntimeException("No implementation for " + clazz);
    }
    return impl.next();
  }

  public static class LauncherException extends RuntimeException
  {
    public LauncherException(String message)
    {
      super(message);
    }

    public LauncherException(Throwable cause)
    {
      super(cause);
    }
  }

}
