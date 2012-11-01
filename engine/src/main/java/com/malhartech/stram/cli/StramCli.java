/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stram.cli;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import javax.ws.rs.core.MediaType;

import jline.ArgumentCompletor;
import jline.Completor;
import jline.ConsoleReader;
import jline.FileNameCompletor;
import jline.History;
import jline.MultiCompletor;
import jline.SimpleCompletor;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Records;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.stram.cli.StramAppLauncher.AppConfig;
import com.malhartech.stram.cli.StramClientUtils.ClientRMHelper;
import com.malhartech.stram.cli.StramClientUtils.YarnClientHelper;
import com.malhartech.stram.webapp.StramWebServices;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 *
 * Provides command line interface for a streaming application on hadoop (yarn)<p>
 * <br>
 * <table border=1 cellspacing=0>
 * <caption>Currently supported Commands</caption>
 * <thead>
 * <tr align=center><th width=10%><b>Command</b></th><th width=20%><b>Parameters</b></th><th width=70%><b>Description</b></th></tr>
 * </thead><tbody>
 * <tr><td><b>help</b></td><td></td><td>prints help on all cli commands</td></tr>
 * <tr><td><b>ls</b></td><td></td><td>lists all current running applications</td></tr>
 * <tr><td><b>connect</b></td><td>appId</td><td>Connects to the given application</td></tr>
 * <tr><td><b>listoperators</b></td><td></td><td>Lists deployed streaming operators</td></tr>
 * <tr><td><b>launch</b></td><td>jarFile [, topologyFile ]</td><td>Launch topology packaged in jar file</td></tr>
 * <tr><td><b>timeout</b></td><td>duration</td><td>Wait for completion of current application</td></tr>
 * <tr><td><b>kill</b></td><td></td><td>Force termination for current application</td></tr>
 * <tr><td><b>exit</b></td><td></td><td>Exit the app</td></tr>
 * </tbody>
 * </table>
 * <br>
 */


public class StramCli
{
  private static final Logger LOG = LoggerFactory.getLogger(StramCli.class);
  private final Configuration conf = new Configuration();
  private final ClientRMHelper rmClient;
  private ApplicationReport currentApp = null;
  private String currentDir = "/";

  private class CliException extends RuntimeException
  {
    private static final long serialVersionUID = 1L;

    CliException(String msg, Throwable cause)
    {
      super(msg, cause);
    }

    CliException(String msg)
    {
      super(msg);
    }
  }

  public StramCli() throws Exception
  {
    YarnClientHelper yarnClient = new YarnClientHelper(conf);
    rmClient = new ClientRMHelper(yarnClient);
  }

  public void init()
  {
  }

  public void run() throws IOException
  {
    printWelcomeMessage();
    ConsoleReader reader = new ConsoleReader();
    reader.setBellEnabled(false);

    String[] commandsList = new String[]{"help", "ls", "cd", "listoperators", "shutdown", "timeout", "kill", "exit"};
    List<Completor> completors = new LinkedList<Completor>();
    completors.add(new SimpleCompletor(commandsList));

    List<Completor> launchCompletors = new LinkedList<Completor>();
    launchCompletors.add(new SimpleCompletor(new String[] {"launch", "launch-local"}));
    launchCompletors.add(new FileNameCompletor()); // jarFile
    launchCompletors.add(new FileNameCompletor()); // topology
    completors.add(new ArgumentCompletor(launchCompletors));

    reader.addCompletor(new MultiCompletor(completors));


    File historyFile = new File(StramClientUtils.getSettingsRootDir(), ".history");
    historyFile.getParentFile().mkdirs();
    try {
      History history = new History(historyFile);
      reader.setHistory(history);
    }
    catch (IOException exp) {
      System.err.printf("Unable to open %s for writing.", historyFile);
    }

    String line;
    PrintWriter out = new PrintWriter(System.out);

    while ((line = readLine(reader, "")) != null) {
      try {
        if ("help".equals(line)) {
          printHelp();
        }
        else if (line.startsWith("ls")) {
          ls(line);
        }
        else if (line.startsWith("connect") || line.startsWith("cd")) {
          connect(line);
        }
        else if (line.startsWith("listoperators")) {
          listOperators(null);
        }
        else if (line.startsWith("launch")) {
          launchApp(line, reader);
        }
        else if (line.startsWith("shutdown")) {
          shutdownApp(line);
        }
        else if (line.startsWith("timeout")) {
          timeoutApp(line, reader);
        }
        else if (line.startsWith("kill")) {
          killApp(line);
        }
        else if ("exit".equals(line)) {
          System.out.println("Exiting application");
          return;
        }
        else {
          System.err.println("Invalid command, For assistance press TAB or type \"help\" then hit ENTER.");
        }
      }
      catch (CliException e) {
        System.err.println(e.getMessage());
        LOG.info("Error processing line: " + line, e);
      }
      catch (Exception e) {
        System.err.println("Unexpected error: " + e.getMessage());
        e.printStackTrace();
      }
      out.flush();
    }
  }

  private void printWelcomeMessage()
  {
    System.out.println("Stram CLI. For assistance press TAB or type \"help\" then hit ENTER.");
  }

  private void printHelp()
  {
    System.out.println("help             - Show help");
    System.out.println("ls               - Show running applications");
    System.out.println("connect <appId>  - Connect to running streaming application");
    System.out.println("listoperators        - List deployed streaming operators");
    System.out.println("launch <jarFile> [<topologyFile>] - Launch topology packaged in jar file.");
    System.out.println("timeout <duration> - Wait for completion of current application.");
    System.out.println("kill             - Force termination for current application.");
    System.out.println("exit             - Exit the app");

  }

  private String readLine(ConsoleReader reader, String promtMessage)
    throws IOException
  {
    String line = reader.readLine(promtMessage + "\nstramcli> ");
    return line.trim();
  }

  private String[] assertArgs(String line, int num, String msg)
  {
    String[] args = StringUtils.splitByWholeSeparator(line, " ");
    if (args.length < num) {
      throw new CliException(msg);
    }
    return args;
  }

  private int getIntArg(String line, int argIndex, String msg)
  {
    String[] args = assertArgs(line, argIndex + 1, msg);
    try {
      int arg = Integer.parseInt(args[argIndex]);
      return arg;
    }
    catch (Exception e) {
      throw new CliException("Not a valid number: " + args[argIndex]);
    }
  }

  private List<ApplicationReport> getApplicationList()
  {
    try {
      GetAllApplicationsRequest appsReq = Records.newRecord(GetAllApplicationsRequest.class);
      return rmClient.clientRM.getAllApplications(appsReq).getApplicationList();
    }
    catch (Exception e) {
      throw new CliException("Error getting application list from resource manager: " + e.getMessage(), e);
    }
  }

  private void ls(String line) throws JSONException
  {
    String[] args = StringUtils.splitByWholeSeparator(line, " ");
    for (int i = args.length; i-- > 0;) {
      args[i] = args[i].trim();
    }

    if (args.length == 2 && args[1].equals("/") || currentDir.equals("/")) {
      listApplications(args);
    }
    else {
      listOperators(args);
    }
  }

  private void listApplications(String[] args)
  {

    try {
      List<ApplicationReport> appList = getApplicationList();
      Collections.sort(appList, new Comparator<ApplicationReport>()
      {
        @Override
        public int compare(ApplicationReport o1, ApplicationReport o2)
        {
          return o1.getApplicationId().getId() - o2.getApplicationId().getId();
        }
      });
      System.out.println("Applications:");
      int totalCnt = 0;
      int runningCnt = 0;

      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

      for (ApplicationReport ar : appList) {
        boolean show;

        /*
         * This is inefficient, but what the heck, if this can be passed through the command line, can anyone notice slowness.
         */
        if (args.length == 1 || args.length == 2 && (args[1].equals("/") || args[1].equals(".."))) {
          show = true;
        }
        else {
          show = false;
          String appid = String.valueOf(ar.getApplicationId().getId());
          for (int i = args.length; i-- > 1;) {
            if (appid.equals(args[i])) {
              show = true;
              break;
            }
          }
        }

        if (show) {
          StringBuilder sb = new StringBuilder();
          sb.append("startTime: ").append(sdf.format(new java.util.Date(ar.getStartTime()))).
            append(", id: ").append(ar.getApplicationId().getId()).
            append(", name: ").append(ar.getName()).
            append(", state: ").append(ar.getYarnApplicationState().name()).
            append(", trackingUrl: ").append(ar.getTrackingUrl()).
            append(", finalStatus: ").append(ar.getFinalApplicationStatus());
          System.out.println(sb);
          totalCnt++;
          if (ar.getYarnApplicationState() == YarnApplicationState.RUNNING) {
            runningCnt++;
          }
        }
      }
      System.out.println(runningCnt + " active, total " + totalCnt + " applications.");
    }
    catch (Exception ex) {
      throw new CliException("Failed to retrieve application list", ex);
    }
  }

  private ClientResponse getResource(String resourcePath)
  {

    if (currentApp == null) {
      throw new CliException("No application selected");
    }

    if (StringUtils.isEmpty(currentApp.getTrackingUrl()) || currentApp.getFinalApplicationStatus() != FinalApplicationStatus.UNDEFINED) {
      currentApp = null;
      currentDir = "/";
      throw new CliException("Application terminated.");
    }

    Client wsClient = Client.create();
    wsClient.setFollowRedirects(true);
    WebResource r = wsClient.resource("http://" + currentApp.getTrackingUrl()).path(StramWebServices.PATH).path(resourcePath);
    try {
      ClientResponse response = r.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      if (!MediaType.APPLICATION_JSON_TYPE.equals(response.getType())) {
        throw new Exception("Unexpected response type " + response.getType());
      }
      return response;
    }
    catch (Exception e) {
      // check the application status as above may have failed due application termination etc.
      try {
        this.currentApp = rmClient.getApplicationReport(currentApp.getApplicationId());
        if (currentApp.getYarnApplicationState() != YarnApplicationState.RUNNING) {
          String msg = String.format("Application %s not running (status %s)",
              currentApp.getApplicationId().getId(), currentApp.getYarnApplicationState());
          throw new CliException(msg);
        }
      } catch (YarnRemoteException rmExc) {
        throw new CliException("Unable to determine application status.", rmExc);
      }
      throw new CliException("Failed to request " + r.getURI(), e);
    }
  }

  private void connect(String line)
  {
    String[] args = StringUtils.splitByWholeSeparator(line, " ");
    if (args.length != 2) {
      System.err.println("Invalid arguments");
      return;
    }

    if ("..".equals(args[1]) || "/".equals(args[1])) {
      currentDir = "/";
      return;
    }
    else {
      currentDir = args[1];
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

    boolean connected = false;
    try {
      LOG.info("Selected {} with tracking url: ", currentApp.getApplicationId(), currentApp.getTrackingUrl());
      ClientResponse rsp = getResource(StramWebServices.PATH_INFO);
      JSONObject json = rsp.getEntity(JSONObject.class);
      System.out.println(json.toString(2));
      connected = true; // set as current only upon successful connection
    }
    catch (CliException e) {
      throw e; // pass on
    }
    catch (JSONException e) {
      throw new CliException("Error connecting to app " + args[1], e);
    } finally {
      if (!connected) {
        //currentApp = null;
        //currentDir = "/";
      }
    }
  }

  private void listOperators(String[] argv) throws JSONException
  {
    ClientResponse rsp = getResource(StramWebServices.PATH_OPERATORS);
    JSONObject json = rsp.getEntity(JSONObject.class);

    if (argv.length > 1) {
      String singleKey = ""+json.keys().next();
      JSONArray matches = new JSONArray();
      // filter operators
      JSONArray arr = json.getJSONArray(singleKey);
      for (int i=0; i<arr.length(); i++) {
        Object val = arr.get(i);
        if (val.toString().matches(argv[1])) {
          matches.put(val);
        }
      }
      json.put(singleKey, matches);
    }

    System.out.println(json.toString(2));
  }

  private void launchApp(String line, ConsoleReader reader)
  {
    String[] args = assertArgs(line, 2, "No jar file specified.");
    boolean localMode = "launch-local".equals(args[0]);

    AppConfig appConfig = null;
    if (args.length == 3) {
      File file = new File(args[2]);
      appConfig = new StramAppLauncher.PropertyFileAppConfig(file);
    }

    try {
      StramAppLauncher submitApp = new StramAppLauncher(new File(args[1]));

      if (appConfig == null) {
        List<AppConfig> cfgList = submitApp.getBundledTopologies();
        if (cfgList.isEmpty()) {
          throw new CliException("No configurations bundled in jar, please specify one");
        }
        else if (cfgList.size() == 1) {
          appConfig = cfgList.get(0);
        }
        else {
          for (int i = 0; i < cfgList.size(); i++) {
            System.out.printf("%3d. %s\n", i + 1, cfgList.get(i).getName());
          }

          boolean useHistory = reader.getUseHistory();
          reader.setUseHistory(false);
          @SuppressWarnings("unchecked")
          List<Completor> completors = new ArrayList<Completor>(reader.getCompletors());
          for (Completor c : completors) {
            reader.removeCompletor(c);
          }
          String optionLine = reader.readLine("Pick configuration? ");
          reader.setUseHistory(useHistory);
          for (Completor c : completors) {
            reader.addCompletor(c);
          }

          try {
            int option = Integer.parseInt(optionLine);
            if (0 < option && option <= cfgList.size()) {
              appConfig = cfgList.get(option - 1);
            }
          }
          catch (Exception e) {
            // ignore
          }
        }
      }

      if (appConfig != null) {
        if (!localMode) {
          ApplicationId appId = submitApp.launchApp(appConfig);
          this.currentApp = rmClient.getApplicationReport(appId);
          this.currentDir = "" + currentApp.getApplicationId().getId();
          System.out.println(appId);
        } else {
          submitApp.runLocal(appConfig);
        }
      }
      else {
        System.err.println("No topology specified.");
      }

    }
    catch (Exception e) {
      throw new CliException("Failed to launch " + args[1] + ": " + e.getMessage(), e);
    }

  }

  private void killApp(String line)
  {
    if (currentApp == null) {
      throw new CliException("No application selected");
    }

    try {
      rmClient.killApplication(currentApp.getApplicationId());
      currentDir = "/";
      currentApp = null;
    }
    catch (YarnRemoteException e) {
      throw new CliException("Failed to kill " + currentApp.getApplicationId(), e);
    }
  }

  private void shutdownApp(String line)
  {
    if (currentApp == null) {
      throw new CliException("No application selected");
    }

    Client wsClient = Client.create();
    wsClient.setFollowRedirects(true);
    // WebAppProxyServlet does not support POST - for now bypass it for this request
    WebResource r = wsClient.resource("http://" + currentApp.getOriginalTrackingUrl()).path(StramWebServices.PATH).path(StramWebServices.PATH_SHUTDOWN);
    try {
      JSONObject response = r.accept(MediaType.APPLICATION_JSON).post(JSONObject.class);
      System.out.println("shutdown requested: " + response);
      currentDir = "/";
      currentApp = null;
    }
    catch (Exception e) {
      throw new CliException("Failed to request " + r.getURI(), e);
    }
  }

  private void timeoutApp(String line, final ConsoleReader reader)
  {
    if (currentApp == null) {
      throw new CliException("No application selected");
    }
    int timeout = getIntArg(line, 1, "Specify wait duration");

    ClientRMHelper.AppStatusCallback cb = new ClientRMHelper.AppStatusCallback()
    {
      @Override
      public boolean exitLoop(ApplicationReport report)
      {
        System.out.println("current status is: " + report.getYarnApplicationState());
        try {
          if (reader.getInput().available() > 0) {
            return true;
          }
        }
        catch (IOException e) {
          LOG.error("Error checking for input.", e);
        }
        return false;
      }
    };

    try {
      boolean result = rmClient.waitForCompletion(currentApp.getApplicationId(), cb, timeout * 1000);
      if (!result) {
        System.err.println("Application terminated unsucessful.");
      }
    }
    catch (YarnRemoteException e) {
      throw new CliException("Failed to kill " + currentApp.getApplicationId(), e);
    }

  }

  public static void main(String[] args) throws Exception
  {
    StramCli shell = new StramCli();
    shell.init();
    shell.run();
  }
}
