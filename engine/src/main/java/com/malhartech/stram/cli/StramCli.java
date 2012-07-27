/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram.cli;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;

import javax.ws.rs.core.MediaType;

import jline.ArgumentCompletor;
import jline.Completor;
import jline.ConsoleReader;
import jline.SimpleCompletor;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.Records;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.stram.cli.StramClientUtils.YarnClientHelper;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class StramCli {
  
  private static Logger LOG = LoggerFactory.getLogger(StramCli.class);
  
  private String[] commandsList;
  private Configuration conf = new Configuration();
  private final YarnClientHelper yarnClient;
  private final ClientRMProtocol rmClient;  
  private ApplicationReport currentApp = null;

  private class CliException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    CliException(String msg, Throwable cause) {
      super(msg, cause);
    }
    CliException(String msg) {
      super(msg);
    }
  }
  
  public StramCli() throws Exception {
    yarnClient = new YarnClientHelper(conf);
    rmClient = yarnClient.connectToASM();
  }
  
  public void init() {
    commandsList = new String[] { "help", "ls", "connect", "exit" };
  }

  public void run() throws IOException {
    printWelcomeMessage();
    ConsoleReader reader = new ConsoleReader();
    reader.setBellEnabled(false);
    List<Completor> completors = new LinkedList<Completor>();

    completors.add(new SimpleCompletor(commandsList));
    reader.addCompletor(new ArgumentCompletor(completors));

    String line;
    PrintWriter out = new PrintWriter(System.out);

    while ((line = readLine(reader, "")) != null) {
      try {
        if ("help".equals(line)) {
          printHelp();
        } else if ("ls".equals(line)) {
          listApplications();
        } else if (line.startsWith("connect")) {
          connect(line);
        } else if (line.startsWith("listnodes")) {
          listNodes(line);
        } else if ("exit".equals(line)) {
          System.out.println("Exiting application");
          return;
        } else {
          System.err
              .println("Invalid command, For assistance press TAB or type \"help\" then hit ENTER.");
        }
      } catch (CliException e) {
        System.err.println(e.getMessage());
        LOG.info("Error processing line: " + line, e);
      } catch (Exception e) {
        System.err.println("Unexpected error: " + e.getMessage());
        e.printStackTrace();
      }
      out.flush();
    }
  }

  private void printWelcomeMessage() {
    System.out
        .println("Stram CLI. For assistance press TAB or type \"help\" then hit ENTER.");
  }

  private void printHelp() {
    System.out.println("help         - Show help");
    System.out.println("ls           - Show currently running applications");
    System.out.println("connect <id> - Connect to running streaming application");
    System.out.println("exit         - Exit the app");

  }

  private String readLine(ConsoleReader reader, String promtMessage)
      throws IOException {
    String line = reader.readLine(promtMessage + "\nstramcli> ");
    return line.trim();
  }
  
  private List<ApplicationReport> getApplicationList() {
    try {
      GetAllApplicationsRequest appsReq = Records.newRecord(GetAllApplicationsRequest.class);
      return rmClient.getAllApplications(appsReq).getApplicationList();
    } catch (Exception e) {
      throw new CliException("Error getting application list from resource manager: " + e.getMessage(), e);
    }
  }
  
  private void listApplications() {
    try {
      List<ApplicationReport> appList = getApplicationList(); 
      System.out.println("Applications:");
      int totalCnt = 0;
      int runningCnt = 0;
      
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      
      for (ApplicationReport ar : appList) {
        StringBuilder sb = new StringBuilder();
        sb.append("startTime: " + sdf.format(new java.util.Date(ar.getStartTime())))
        .append(", id: " + ar.getApplicationId().getId())
        .append(", name: " + ar.getName())
        .append(", state: " + ar.getYarnApplicationState().name())
        .append(", trackingUrl: " + ar.getTrackingUrl());
        System.out.println(sb);
        //Map<?,?> properties = BeanUtils.describe(ar);
        //System.out.println(properties);
        totalCnt++;
        if (ar.getYarnApplicationState() == YarnApplicationState.RUNNING) {
          runningCnt++;
        }
      }
      System.out.println(runningCnt + " active, total " + totalCnt + " applications.");
    } catch (Exception ex) {
      throw new CliException("Failed to retrieve application list", ex);
    }
  }

  private ClientResponse getResource(String resourcePath) {

    if (currentApp == null) {
      throw new CliException("No application selected");
    }
    
    Client wsClient = Client.create();
    wsClient.setFollowRedirects(true);
    WebResource r = wsClient.resource("http://" + currentApp.getTrackingUrl())
        .path("ws").path("v1").path("stram").path(resourcePath);
    try {
      ClientResponse response = r.accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
      if (!MediaType.APPLICATION_JSON_TYPE.equals(response.getType())) {
        throw new Exception("Unexpected response type " + response.getType());
      }
      return response;
    } catch (Exception e) {
      throw new CliException("Failed to request " + r.getURI(), e);
    }
  }
  
  private void connect(String line) {
    String[] args = StringUtils.splitByWholeSeparator(line, " ");
    if (args.length != 2) {
      System.err.println("Invalid arguments");
      return;
    }

    int appSeq = Integer.parseInt(args[1]);
    
    List<ApplicationReport> appList = getApplicationList(); 
    for (ApplicationReport ar : appList) {
      if (ar.getApplicationId().getId() == appSeq) {
        currentApp = ar;
        break;
      }
    }
    if (currentApp == null) {
      throw new CliException("Invalid application id: " + args[1]);
    }

    try {
      LOG.info("Selected {} with tracking url: ", currentApp.getApplicationId(), currentApp.getTrackingUrl());
      ClientResponse rsp = getResource("info");
      JSONObject json = rsp.getEntity(JSONObject.class);      
      System.out.println(json);
    } catch (Exception e) {
      currentApp = null;
      throw new CliException("Error connecting to app " + args[1], e);
    }
  }

  private void listNodes(String line) {

    ClientResponse rsp = getResource("nodes");
    JSONObject json = rsp.getEntity(JSONObject.class);      
    System.out.println(json);

  }
  
  public static void main(String[] args) throws Exception {
    StramCli shell = new StramCli();
    shell.init();
    shell.run();
  }
}
