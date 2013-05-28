/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stram.cli;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import jline.ArgumentCompletor;
import jline.Completor;
import jline.ConsoleReader;
import jline.FileNameCompletor;
import jline.History;
import jline.MultiCompletor;
import jline.SimpleCompletor;

import javax.ws.rs.core.MediaType;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Records;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.stram.cli.StramAppLauncher.AppConfig;
import com.malhartech.stram.cli.StramClientUtils.ClientRMHelper;
import com.malhartech.stram.cli.StramClientUtils.YarnClientHelper;
import com.malhartech.stram.plan.logical.*;
import com.malhartech.stram.security.StramUserLogin;
import com.malhartech.stram.webapp.StramWebServices;
import com.malhartech.util.VersionInfo;
import com.malhartech.util.WebServicesClient;
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
 * <tr><td><b>ls</b></td><td></td><td>list applications or operators</td></tr>
 * <tr><td><b>cd</b></td><td>appId</td><td>connect to the given application</td></tr>
 * <tr><td><b>launch</b></td><td>jarFile</td><td>Launch application packaged in jar file</td></tr>
 * <tr><td><b>timeout</b></td><td>duration</td><td>Wait for completion of current application</td></tr>
 * <tr><td><b>kill</b></td><td></td><td>Force termination for current application</td></tr>
 * <tr><td><b>exit</b></td><td></td><td>Exit the app</td></tr>
 * </tbody>
 * </table>
 * <br>
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public class StramCli
{
  private static final Logger LOG = LoggerFactory.getLogger(StramCli.class);
  private final Configuration conf = new Configuration();
  private ClientRMHelper rmClient;
  private ApplicationReport currentApp = null;
  private String currentDir = "/";

  protected ApplicationReport getApplication(int appSeq)
  {
    List<ApplicationReport> appList = getApplicationList();
    for (ApplicationReport ar: appList) {
      if (ar.getApplicationId().getId() == appSeq) {
        return ar;
      }
    }

    return null;
  }

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

  public StramCli()
  {
    StramClientUtils.addStramResources(conf);
  }

  public void init() throws IOException
  {
    // Need to initialize security before starting RPC for the credentials to
    // take effect
    StramUserLogin.attemptAuthentication(conf);
    YarnClientHelper yarnClient = new YarnClientHelper(conf);
    rmClient = new ClientRMHelper(yarnClient);
  }

  /**
   * Why reinvent the wheel?
   * JLine 2.x supports search and more.. but it uses the same package as JLine 1.x
   * Hadoop bundles and forces 1.x into our class path (when CLI is launched via hadoop command).
   */
  private class ConsoleReaderExt extends ConsoleReader
  {
    private final char REVERSE_SEARCH_KEY = (char)31;

    ConsoleReaderExt() throws IOException
    {
      // CTRL-/ since CTRL-R already mapped to redisplay
      addTriggeredAction(REVERSE_SEARCH_KEY, new ActionListener()
      {
        @Override
        public void actionPerformed(ActionEvent e)
        {
          try {
            searchHistory();
          }
          catch (IOException ex) {
          }
        }

      });
    }

    public int searchBackwards(CharSequence searchTerm, int startIndex)
    {
      @SuppressWarnings("unchecked")
      List<String> history = getHistory().getHistoryList();
      if (startIndex < 0) {
        startIndex = history.size();
      }
      for (int i = startIndex; --i > 0;) {
        String line = history.get(i);
        if (line.contains(searchTerm)) {
          return i;
        }
      }
      return -1;
    }

    private void searchHistory() throws IOException
    {
      final String prompt = "reverse-search: ";
      StringBuilder searchTerm = new StringBuilder();
      String matchingCmd = null;
      int historyIndex = -1;
      while (true) {
        while (backspace()) {
          continue;
        }
        String line = prompt + searchTerm;
        if (matchingCmd != null) {
          line = line.concat(": ").concat(matchingCmd);
        }
        this.putString(line);

        int c = this.readVirtualKey();
        if (c == 8) {
          if (searchTerm.length() > 0) {
            searchTerm.deleteCharAt(searchTerm.length() - 1);
          }
        }
        else if (c == REVERSE_SEARCH_KEY) {
          int newIndex = searchBackwards(searchTerm, historyIndex);
          if (newIndex >= 0) {
            historyIndex = newIndex;
            matchingCmd = (String)getHistory().getHistoryList().get(historyIndex);
          }
        }
        else if (!Character.isISOControl(c)) {
          searchTerm.append(Character.toChars(c));
          int newIndex = searchBackwards(searchTerm, -1);
          if (newIndex >= 0) {
            historyIndex = newIndex;
            matchingCmd = (String)getHistory().getHistoryList().get(historyIndex);
          }
        }
        else {
          while (backspace()) {
            continue;
          }
          if (!StringUtils.isBlank(matchingCmd)) {
            this.putString(matchingCmd);
            this.flushConsole();
          }
          return;
        }
      }
    }

  }

  public void run() throws IOException
  {
    printWelcomeMessage();
    ConsoleReader reader = new ConsoleReaderExt();
    reader.setBellEnabled(false);

    String[] commandsList = new String[] {"help", "ls", "cd", "shutdown", "timeout", "kill", "container-kill",
                                          "startrecording", "stoprecording", "syncrecording", "operator-property-set",
                                          "begin-logical-plan-change",
                                          "exit"};
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
        else if (line.startsWith("cd")) {
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
        else if (line.startsWith("container-kill")) {
          killContainer(line);
        }
        else if (line.startsWith("startrecording")) {
          startRecording(line);
        }
        else if (line.startsWith("stoprecording")) {
          stopRecording(line);
        }
        else if (line.startsWith("syncrecording")) {
          syncRecording(line);
        }
        else if (line.startsWith("operator-property-set")) {
          setOperatorProperty(line);
        }
        else if (line.startsWith("begin-logical-plan-change")) {
          beginLogicalPlanChange(line, reader);
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
      }
      out.flush();
    }
    System.out.println("exit");
  }

  private void printWelcomeMessage()
  {
    System.out.println("Stram CLI " + VersionInfo.getVersion() + " " + VersionInfo.getDate() + " " + VersionInfo.getRevision());
  }

  private void printHelp()
  {
    System.out.println("help             - Show help");
    System.out.println("ls               - List applications or operators");
    System.out.println("connect <appId>  - Connect to running streaming application");
    System.out.println("launch <jarFile> [<configuration>] - Launch application packaged in jar file.");
    System.out.println("timeout <duration> - Wait for completion of current application.");
    System.out.println("kill             - Force termination for current application.");
    System.out.println("startrecording <operId> [<portName>] - Start recording tuples for the given operator id");
    System.out.println("stoprecording <operId> [<portName>] - Stop recording tuples for the given operator id");
    System.out.println("syncrecording <operId> [<portName>] - Sync recording tuples for the given operator id");
    System.out.println("operator-property-set <operId> <name> <value> - Set the property of the given operator id");
    System.out.println("begin-logical-plan-change - Begin changing the logical plan for the current application");
    System.out.println("exit             - Exit the app");

  }

  private void printHelpLogicalPlanChange()
  {
    System.out.println("help              - Show help");
    System.out.println("create-operator <name> <class> - Create an operator with the given name and the given class");
    System.out.println("remove-operator <name>         - Remove an operator with the given name");
    System.out.println("create-stream <name> <operator-source-name> <operator-source-port-name> <operator-sink-name> <operator-sink-port-name> - Create a stream");
    System.out.println("remove-stream <name");
    System.out.println("operator-property-set <name> <property-name> <property-value> - Set the property of the given operator name");
    System.out.println("operator-attribute-set <name> <attribute-name> <attribute-value> - Set the attribute of the given operator name");
    System.out.println("port-attribute-set <operator-name> <port-name> <attribute-name> <attribute-value> - Set the attribute of the given port of the given operator");
    System.out.println("abort             - Abort the logical plan change");
    System.out.println("submit            - Submit the logical plan change");
  }

  private String readLine(ConsoleReader reader, String promtMessage)
          throws IOException
  {
    String line = reader.readLine(promtMessage + "\nstramcli> ");
    if (line == null) {
      return null;
    }
    return line.trim();
  }

  private String[] assertArgs(String line, int num, String msg)
  {
    line = line.trim();
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

    String context = (args.length > 1 ? args[1] : currentDir);
    if (context.equals("/")) {
      listApplications(args);
    }
    else if (context.startsWith("container")) {
      listContainers(Arrays.copyOfRange(args, 2, args.length));
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

      for (ApplicationReport ar: appList) {
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

  private ApplicationReport assertRunningApp(ApplicationReport app)
  {
    ApplicationReport r;
    try {
      r = rmClient.getApplicationReport(app.getApplicationId());
      if (r.getYarnApplicationState() != YarnApplicationState.RUNNING) {
        String msg = String.format("Application %s not running (status %s)",
                                   r.getApplicationId().getId(), r.getYarnApplicationState());
        throw new CliException(msg);
      }
    }
    catch (YarnRemoteException rmExc) {
      throw new CliException("Unable to determine application status.", rmExc);
    }
    return r;
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

    WebServicesClient wsClient = new WebServicesClient();
    Client client = wsClient.getClient();
    client.setFollowRedirects(true);
    WebResource r = client.resource("http://" + currentApp.getTrackingUrl()).path(StramWebServices.PATH).path(resourcePath);
    try {
      return wsClient.process(r, ClientResponse.class, new WebServicesClient.WebServicesHandler<ClientResponse>() {

        @Override
        public ClientResponse process(WebResource webResource, Class<ClientResponse> clazz)
        {
          ClientResponse response = webResource.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
          if (!MediaType.APPLICATION_JSON_TYPE.equals(response.getType())) {
            //throw new Exception("Unexpected response type " + response.getType());
          }
          return response;
        }
      });
    }
    catch (Exception e) {
      // check the application status as above may have failed due application termination etc.
      currentApp = assertRunningApp(currentApp);
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

    currentApp = getApplication(Integer.parseInt(currentDir));
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
    }
    finally {
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
      String singleKey = "" + json.keys().next();
      JSONArray matches = new JSONArray();
      // filter operators
      JSONArray arr = json.getJSONArray(singleKey);
      for (int i = 0; i < arr.length(); i++) {
        Object val = arr.get(i);
        if (val.toString().matches(argv[1])) {
          matches.put(val);
        }
      }
      json.put(singleKey, matches);
    }

    System.out.println(json.toString(2));
  }

  private void listContainers(String[] args) throws JSONException
  {
    ClientResponse rsp = getResource(StramWebServices.PATH_CONTAINERS);
    JSONObject json = rsp.getEntity(JSONObject.class);
    if (args == null || args.length == 0) {
      System.out.println(json.toString(2));
    }
    else {
      JSONArray containers = json.getJSONArray("containers");
      if (containers == null) {
        System.out.println("No containers found!");
      }
      else {
        for (int o = containers.length(); o-- > 0;) {
          JSONObject container = containers.getJSONObject(o);
          String id = container.getString("id");
          if (id != null && !id.isEmpty()) {
            for (int argc = args.length; argc-- > 0;) {
              String s1 = "0" + args[argc];
              String s2 = "_" + args[argc];
              if (id.endsWith(s1) || id.endsWith(s2)) {
                System.out.println(container.toString(2));
              }
            }
          }
        }
      }
    }
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
          throw new CliException("No applications bundled in jar, please specify one");
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
          for (Completor c: completors) {
            reader.removeCompletor(c);
          }
          String optionLine = reader.readLine("Pick application? ");
          reader.setUseHistory(useHistory);
          for (Completor c: completors) {
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
        }
        else {
          submitApp.runLocal(appConfig);
        }
      }
      else {
        System.err.println("No application specified.");
      }

    }
    catch (Exception e) {
      throw new CliException("Failed to launch " + args[1] + ": " + e.getMessage(), e);
    }

  }

  @SuppressWarnings({"null", "ConstantConditions"})
  private void killApp(String line)
  {
    String[] args = StringUtils.splitByWholeSeparator(line, " ");
    if (args.length == 1) {
      if (currentApp == null) {
        throw new CliException("No application selected");
      }
      else {
        try {
          rmClient.killApplication(currentApp.getApplicationId());
          currentDir = "/";
          currentApp = null;
        }
        catch (YarnRemoteException e) {
          throw new CliException("Failed to kill " + currentApp.getApplicationId(), e);
        }
      }

      return;
    }

    ApplicationReport app = null;
    int i = 0;
    try {
      while (++i < args.length) {
        app = getApplication(Integer.parseInt(args[i]));
        rmClient.killApplication(app.getApplicationId());
        if (app == currentApp) {
          currentDir = "/";
          currentApp = null;
        }
      }
    }
    catch (YarnRemoteException e) {
      throw new CliException("Failed to kill " + (app.getApplicationId() == null? "unknown application": app.getApplicationId()) + ". Aborting killing of any additional applications.", e);
    }
    catch (NumberFormatException nfe) {
      throw new CliException("Invalid application Id " + args[i], nfe);
    }
    catch (NullPointerException npe) {
      throw new CliException("Application with Id " + args[i] + " does not seem to be alive!", npe);
    }

  }

  private WebResource getPostResource(WebServicesClient webServicesClient)
  {
    if (currentApp == null) {
      throw new CliException("No application selected");
    }
    // YARN-156 WebAppProxyServlet does not support POST - for now bypass it for this request
    currentApp = assertRunningApp(currentApp); // or else "N/A" might be there..
    String trackingUrl = currentApp.getOriginalTrackingUrl();

    Client wsClient = webServicesClient.getClient();
    wsClient.setFollowRedirects(true);
    return wsClient.resource("http://" + trackingUrl).path(StramWebServices.PATH);
  }

  private void shutdownApp(String line)
  {
    WebServicesClient webServicesClient = new WebServicesClient();
    WebResource r = getPostResource(webServicesClient).path(StramWebServices.PATH_SHUTDOWN);
    try {
      JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
      {

        @Override
        public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
        {
          return webResource.accept(MediaType.APPLICATION_JSON).post(clazz);
        }
      });
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

  private void killContainer(String line)
  {
    if (currentApp == null) {
      throw new CliException("No application selected");
    }
    String[] args = assertArgs(line, 2, "no container id specified.");
    WebServicesClient webServicesClient = new WebServicesClient();
    WebResource r = getPostResource(webServicesClient).path(StramWebServices.PATH_CONTAINERS).path(args[1]).path("kill");
    try {
      JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
      {
        @Override
        public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
        {
          return webResource.accept(MediaType.APPLICATION_JSON).post(clazz, new JSONObject());
        }
      });
      System.out.println("container stop requested: " + response);
    }
    catch (Exception e) {
      throw new CliException("Failed to request " + r.getURI(), e);
    }
  }

  private void startRecording(String line)
  {
    if (currentApp == null) {
      throw new CliException("No application selected");
    }
    String[] args = StringUtils.splitByWholeSeparator(line, " ");
    if (args.length != 2 && args.length != 3) {
      System.err.println("Invalid arguments");
      return;
    }

    WebServicesClient webServicesClient = new WebServicesClient();
    WebResource r = getPostResource(webServicesClient).path(StramWebServices.PATH_STARTRECORDING);
    final JSONObject request = new JSONObject();
    try {
      request.put("operId", args[1]);
      if (args.length == 3) {
        request.put("portName", args[2]);
      }
      JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
      {

        @Override
        public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
        {
          return webResource.accept(MediaType.APPLICATION_JSON).post(clazz, request);
        }
      });
      System.out.println("start recording requested: " + response);
    }
    catch (Exception e) {
      throw new CliException("Failed to request " + r.getURI(), e);
    }
  }

  private void stopRecording(String line)
  {
    if (currentApp == null) {
      throw new CliException("No application selected");
    }
    String[] args = StringUtils.splitByWholeSeparator(line, " ");
    if (args.length != 2 && args.length != 3) {
      System.err.println("Invalid arguments");
      return;
    }

    WebServicesClient webServicesClient = new WebServicesClient();
    WebResource r = getPostResource(webServicesClient).path(StramWebServices.PATH_STOPRECORDING);
    final JSONObject request = new JSONObject();

    try {
      request.put("operId", args[1]);
      if (args.length == 3) {
        request.put("portName", args[2]);
      }
      JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>() {

        @Override
        public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
        {
          return webResource.accept(MediaType.APPLICATION_JSON).post(clazz, request);
        }
      });
      System.out.println("stop recording requested: " + response);
    }
    catch (Exception e) {
      throw new CliException("Failed to request " + r.getURI(), e);
    }
  }

  private void syncRecording(String line)
  {
    if (currentApp == null) {
      throw new CliException("No application selected");
    }
    String[] args = StringUtils.splitByWholeSeparator(line, " ");
    if (args.length != 2 && args.length != 3) {
      System.err.println("Invalid arguments");
      return;
    }

    WebServicesClient webServicesClient = new WebServicesClient();
    WebResource r = getPostResource(webServicesClient).path(StramWebServices.PATH_SYNCRECORDING);
    final JSONObject request = new JSONObject();

    try {
      request.put("operId", args[1]);
      if (args.length == 3) {
        request.put("portName", args[2]);
      }
      JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>() {

        @Override
        public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
        {
          return webResource.accept(MediaType.APPLICATION_JSON).post(clazz, request);
        }
      });
      System.out.println("sync recording requested: " + response);
    }
    catch (Exception e) {
      throw new CliException("Failed to request " + r.getURI(), e);
    }
  }

  private void setOperatorProperty(String line)
  {
    if (currentApp == null) {
      throw new CliException("No application selected");
    }
    String[] args = assertArgs(line, 4, "required arguments: <operatorName> <propertyName> <propertyValue>");
    WebServicesClient webServicesClient = new WebServicesClient();
    WebResource r = getPostResource(webServicesClient).path(StramWebServices.PATH_LOGICAL_PLAN_OPERATORS).path(args[1]).path("setProperty");
    try {
      final JSONObject request = new JSONObject();
      request.put("propertyName", args[2]);
      request.put("propertyValue", args[3]);
      JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
      {

        @Override
        public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
        {
          return webResource.accept(MediaType.APPLICATION_JSON).post(JSONObject.class, request);
        }
      });
      System.out.println("request submitted: " + response);
    }
    catch (Exception e) {
      throw new CliException("Failed to request " + r.getURI(), e);
    }
  }

  private void beginLogicalPlanChange(String line, ConsoleReader reader)
  {
    if (currentApp == null) {
      throw new CliException("No application selected");
    }
    try {
      List<LogicalPlanRequest> requests = new ArrayList<LogicalPlanRequest>();
      while (true) {
        String line2 = reader.readLine("logical-plan-change> ");
        LogicalPlanRequest request = null;
        if ("help".equals(line2)) {
          printHelpLogicalPlanChange();
        }
        else if ("submit".equals(line2)) {
          // submit change
          System.out.println("Logical plan change submitted.");
          submitLogicalPlanChange(requests);
          return;
        }
        else if ("abort".equals(line2)) {
          System.out.println("Logical plan change aborted.");
          return;
        }
        else if (line2.startsWith("create-operator")) {
          request = logicalPlanCreateOperatorRequest(line2);
        }
        else if (line2.startsWith("remove-operator")) {
          request = logicalPlanRemoveOperatorRequest(line2);
        }
        else if (line2.startsWith("create-stream")) {
          request = logicalPlanCreateStreamRequest(line2);
        }
        else if (line2.startsWith("remove-stream")) {
          request = logicalPlanRemoveStreamRequest(line2);
        }
        else if (line2.startsWith("operator-attribute-set")) {
          request = logicalPlanOperatorAttributeSetRequest(line2);
        }
        else if (line2.startsWith("operator-property-set")) {
          request = logicalPlanOperatorPropertySetRequest(line2);
        }
        else if (line2.startsWith("port-attribute-set")) {
          request = logicalPlanPortAttributeSetRequest(line2);
        }
        else if (line2.startsWith("stream-attribute-set")) {
          request = logicalPlanStreamAttributeSetRequest(line2);
        }
        else if (line2.startsWith("queue")) {
          ObjectMapper mapper = new ObjectMapper();
          System.out.println(mapper.defaultPrettyPrintingWriter().writeValueAsString(requests));
          System.out.println("Total operations in queue: " + requests.size());
        }
        else {
          System.out.println("Invalid command. Ignored.");
        }
        if (request != null) {
          requests.add(request);
        }
      }
    } catch (Exception ex) {
      throw new CliException("Failed to submit logical plan change", ex);
    }
  }

  private LogicalPlanRequest logicalPlanCreateOperatorRequest(String line)
  {
    String[] args = assertArgs(line, 3, "required arguments: <operatorName> <className>");
    String operatorName = args[1];
    String className = args[2];
    CreateOperatorRequest request = new CreateOperatorRequest();
    request.setOperatorName(operatorName);
    request.setOperatorFQCN(className);
    return request;
  }

  private LogicalPlanRequest logicalPlanRemoveOperatorRequest(String line)
  {
    String[] args = assertArgs(line, 2, "required arguments: <operatorName>");
    String operatorName = args[1];
    RemoveOperatorRequest request = new RemoveOperatorRequest();
    request.setOperatorName(operatorName);
    return request;
  }

  private LogicalPlanRequest logicalPlanCreateStreamRequest(String line)
  {
    String[] args = assertArgs(line, 6, "required arguments: <streamName> <sourceOperatorName> <sourcePortName> <sinkOperatorName> <sinkPortName>");
    String streamName = args[1];
    String sourceOperatorName = args[2];
    String sourcePortName = args[3];
    String sinkOperatorName = args[4];
    String sinkPortName = args[5];
    CreateStreamRequest request = new CreateStreamRequest();
    request.setStreamName(streamName);
    request.setSourceOperatorName(sourceOperatorName);
    request.setSinkOperatorName(sinkOperatorName);
    request.setSourceOperatorPortName(sourcePortName);
    request.setSinkOperatorPortName(sinkPortName);
    return request;
  }

  private LogicalPlanRequest logicalPlanRemoveStreamRequest(String line)
  {
    String[] args = assertArgs(line, 2, "required arguments: <streamName>");
    String streamName = args[1];
    RemoveStreamRequest request = new RemoveStreamRequest();
    request.setStreamName(streamName);
    return request;
  }

  private LogicalPlanRequest logicalPlanOperatorAttributeSetRequest(String line)
  {
    String[] args = assertArgs(line, 4, "required arguments: <operatorName> <attributeName> <attributeValue>");
    String operatorName = args[1];
    String attributeName = args[2];
    String attributeValue = args[3];
    OperatorAttributeSetRequest request = new OperatorAttributeSetRequest();
    request.setOperatorName(operatorName);
    request.setAttributeName(attributeName);
    request.setAttributeValue(attributeValue);
    return request;
  }

  private LogicalPlanRequest logicalPlanOperatorPropertySetRequest(String line)
  {
    String[] args = assertArgs(line, 4, "required arguments: <operatorName> <propertyName> <propertyValue>");
    String operatorName = args[1];
    String propertyName = args[2];
    String propertyValue = args[3];
    OperatorPropertySetRequest request = new OperatorPropertySetRequest();
    request.setOperatorName(operatorName);
    request.setPropertyName(propertyName);
    request.setPropertyValue(propertyValue);
    return request;
  }

  private LogicalPlanRequest logicalPlanPortAttributeSetRequest(String line)
  {
    String[] args = assertArgs(line, 5, "required arguments: <operatorName> <portName> <attributeName> <attributeValue>");
    String operatorName = args[1];
    String attributeName = args[2];
    String attributeValue = args[3];
    PortAttributeSetRequest request = new PortAttributeSetRequest();
    request.setOperatorName(operatorName);
    request.setAttributeName(attributeName);
    request.setAttributeValue(attributeValue);
    return request;
  }

  private LogicalPlanRequest logicalPlanStreamAttributeSetRequest(String line)
  {
    String[] args = assertArgs(line, 4, "required arguments: <streamName> <attributeName> <attributeValue>");
    String streamName = args[1];
    String attributeName = args[2];
    String attributeValue = args[3];
    StreamAttributeSetRequest request = new StreamAttributeSetRequest();
    request.setStreamName(streamName);
    request.setAttributeName(attributeName);
    request.setAttributeValue(attributeValue);
    return request;
  }

  private void submitLogicalPlanChange(List<LogicalPlanRequest> requests)
  {
    if (currentApp == null) {
      throw new CliException("No application selected");
    }
    if (requests.isEmpty()) {
      throw new CliException("Nothing to submit");
    }
    WebServicesClient webServicesClient = new WebServicesClient();
    WebResource r = getPostResource(webServicesClient).path(StramWebServices.PATH_LOGICAL_PLAN_MODIFICATION);
    try {
      final Map<String, Object> m = new HashMap<String, Object>();
      ObjectMapper mapper = new ObjectMapper();
      m.put("requests", requests);
      final JSONObject jsonRequest = new JSONObject(mapper.writeValueAsString(m));

      JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
      {

        @Override
        public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
        {
          return webResource.accept(MediaType.APPLICATION_JSON).post(JSONObject.class, jsonRequest);
        }
      });

      System.out.println("request submitted: " + response);
    }
    catch (Exception e) {
      throw new CliException("Failed to request " + r.getURI(), e);
    }
  }


  public static void main(String[] args) throws Exception
  {
    StramCli shell = new StramCli();
    shell.init();
    shell.run();
  }

}
