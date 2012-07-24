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
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.api.json.JSONJAXBContext;

/**
 * Application master webapp
 */
public class StramWebApp extends WebApp {

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
/*    
    route("/", AppController.class);
    route("/app", AppController.class);
    route(pajoin("/job", JOB_ID), AppController.class, "job");
    route(pajoin("/conf", JOB_ID), AppController.class, "conf");
    route(pajoin("/jobcounters", JOB_ID), AppController.class, "jobCounters");
    route(pajoin("/singlejobcounter",JOB_ID, COUNTER_GROUP, COUNTER_NAME),
        AppController.class, "singleJobCounter");
    route(pajoin("/tasks", JOB_ID, TASK_TYPE), AppController.class, "tasks");
    route(pajoin("/attempts", JOB_ID, TASK_TYPE, ATTEMPT_STATE),
        AppController.class, "attempts");
    route(pajoin("/task", TASK_ID), AppController.class, "task");
    route(pajoin("/taskcounters", TASK_ID), AppController.class, "taskCounters");
    route(pajoin("/singletaskcounter",TASK_ID, COUNTER_GROUP, COUNTER_NAME),
        AppController.class, "singleTaskCounter");
*/        
  }
}
