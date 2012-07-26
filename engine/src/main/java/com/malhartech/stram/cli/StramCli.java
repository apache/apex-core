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

import com.malhartech.stram.cli.StramClientUtils.YarnClientHelper;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class StramCli {
  private String[] commandsList;
  private Configuration conf = new Configuration();
  private final YarnClientHelper yarnClient;
  private final ClientRMProtocol rmClient;  
  
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
      if ("help".equals(line)) {
        printHelp();
      } else if ("ls".equals(line)) {
        listApplications();
      } else if (line.startsWith("connect")) {
        connect(line);
      } else if ("exit".equals(line)) {
        System.out.println("Exiting application");
        return;
      } else {
        System.out
            .println("Invalid command, For assistance press TAB or type \"help\" then hit ENTER.");
      }
      out.flush();
    }
  }

  private void printWelcomeMessage() {
    System.out
        .println("Stram CLI. For assistance press TAB or type \"help\" then hit ENTER.");

  }

  private List<ApplicationReport> getApplicationList() throws Exception {
    GetAllApplicationsRequest appsReq = Records.newRecord(GetAllApplicationsRequest.class);
    return rmClient.getAllApplications(appsReq).getApplicationList();
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
      System.err.println("Failed to retrieve application list:");
      ex.printStackTrace(System.err);
    }
  }

  private void connect(String line) {
    String[] args = StringUtils.splitByWholeSeparator(line, " ");
    if (args.length != 2) {
      System.err.println("Invalid arguments");
      return;
    }

    try {
      int appSeq = Integer.parseInt(args[1]);
      
      List<ApplicationReport> appList = getApplicationList(); 
      ApplicationReport selectedApp = null;
      for (ApplicationReport ar : appList) {
        if (ar.getApplicationId().getId() == appSeq) {
          selectedApp = ar;
          break;
        }
      }
      
      if (selectedApp == null) {
        System.err.println("Invalid application id: " + args[1]);
        return;
      }
      System.out.println("Selected " + selectedApp.getApplicationId() + " " + selectedApp.getTrackingUrl());

      Client wsClient = Client.create();
      wsClient.setFollowRedirects(true);
      WebResource r = wsClient.resource("http://" + selectedApp.getTrackingUrl())
          .path("ws").path("v1").path("stram").path("info");
      try {
        ClientResponse response = r.accept(MediaType.APPLICATION_JSON)
            .get(ClientResponse.class);
        //assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
        JSONObject json = response.getEntity(JSONObject.class);      
        System.out.println(json);
      } catch (Exception e) {
        System.err.println("Error connecting to " + r.getURI());
        e.printStackTrace(System.err);
      }
      
    } catch (Exception e) {
      System.err.println("Error connecting to app " + args[1]);
      e.printStackTrace(System.err);
    }
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

  public static void main(String[] args) throws Exception {
    StramCli shell = new StramCli();
    shell.init();
    shell.run();
  }
}
