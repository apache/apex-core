/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram.webapp;


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
import com.malhartech.stram.DNodeManager;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.api.json.JSONJAXBContext;

/**
 * The web services interface in the stram. All web services are routed through this interface<p>
 * This class does all the wiring and controls the context. The implementation is in StramWebServices class
 * 
 */
public class StramWebApp extends WebApp {

  final private DNodeManager topologyManager;

  public StramWebApp(DNodeManager topolManager) {
    this.topologyManager = topolManager;
  }
  
  @Singleton
  @Provider
  public static class JAXBContextResolver implements ContextResolver<JAXBContext> {

    private JAXBContext context;
    private final Set<Class<?>> types;

    // you have to specify all the dao classes here
    private final Class<?>[] cTypes = {
      AppInfo.class, RemoteExceptionData.class};

    public JAXBContextResolver() throws Exception {
      this.types = new HashSet<Class<?>>(Arrays.asList(cTypes));
      this.context = new JSONJAXBContext(JSONConfiguration.natural().
          rootUnwrapping(false).build(), cTypes);
    }

    @Override
    public JAXBContext getContext(Class<?> type) {
      return (types.contains(type)) ? context : null;
    }
    
  }  
  
  @Override
  public void setup() {
    bind(JAXBContextResolver.class);
    bind(GenericExceptionHandler.class);
    bind(StramWebServices.class);
    bind(DNodeManager.class).toInstance(this.topologyManager);
  }
}
