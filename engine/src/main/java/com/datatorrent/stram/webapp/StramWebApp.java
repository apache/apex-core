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
package com.datatorrent.stram.webapp;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBContext;

import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.RemoteExceptionData;
import org.apache.hadoop.yarn.webapp.WebApp;

import com.google.inject.Singleton;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.api.json.JSONJAXBContext;

import com.datatorrent.stram.StreamingContainerManager;

/**
 *
 * The web services interface in the stram. All web services are routed through this interface<p>
 * <br>
 * This class does all the wiring and controls the context. The implementation is in StramWebServices class<br>
 * <br>
 *
 * @since 0.3.2
 */
public class StramWebApp extends WebApp
{

  private final StreamingContainerManager moduleManager;

  /**
   * @param topolManager
   */
  public StramWebApp(StreamingContainerManager topolManager)
  {
    this.moduleManager = topolManager;
  }

  @Singleton
  @Provider
  public static class JAXBContextResolver implements ContextResolver<JAXBContext>
  {

    private final JAXBContext context;
    private final Set<Class<?>> types;

    // you have to specify all the dao classes here
    private final Class<?>[] cTypes = {
        AppInfo.class, RemoteExceptionData.class
    };

    /**
     * @throws Exception
     */
    public JAXBContextResolver() throws Exception
    {
      this.types = new HashSet<>(Arrays.asList(cTypes));
      this.context = new JSONJAXBContext(JSONConfiguration.natural().rootUnwrapping(false).build(), cTypes);
    }

    /**
     * @param type
     * @return JAXContext
     */
    @Override
    public JAXBContext getContext(Class<?> type)
    {
      return (types.contains(type)) ? context : null;
    }

  }

  /**
   *
   */
  @Override
  public void setup()
  {
    bind(JAXBContextResolver.class);
    bind(GenericExceptionHandler.class);
    bind(WebServices.class);
    bind(StramWebServices.class);
    bind(StreamingContainerManager.class).toInstance(this.moduleManager);
  }
}
