/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stram.cli;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import jline.ArgumentCompletor;
import jline.Completor;
import jline.ConsoleReader;
import jline.FileNameCompletor;
import jline.History;
import jline.MultiCompletor;
import jline.SimpleCompletor;

import javax.ws.rs.core.MediaType;

import org.apache.commons.cli.*;
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
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public class StramCli
{
  private static final Logger LOG = LoggerFactory.getLogger(StramCli.class);
  private final Configuration conf = new Configuration();
  private ClientRMHelper rmClient;
  private ApplicationReport currentApp = null;
  private boolean consolePresent;
  private String[] commandsToExecute;
  private final Map<String, CommandSpec> globalCommands = new TreeMap<String, CommandSpec>();
  private final Map<String, CommandSpec> connectedCommands = new TreeMap<String, CommandSpec>();
  private final Map<String, CommandSpec> logicalPlanChangeCommands = new TreeMap<String, CommandSpec>();
  private final Map<String, String> aliases = new HashMap<String, String>();
  private final Map<String, List<String>> macros = new HashMap<String, List<String>>();
  private boolean changingLogicalPlan = false;
  List<LogicalPlanRequest> logicalPlanRequestQueue = new ArrayList<LogicalPlanRequest>();

  private interface Command
  {
    void execute(String[] args, ConsoleReader reader) throws Exception;

  }

  private static class CommandSpec
  {
    Command command;
    String[] requiredArgs;
    String[] optionalArgs;
    String description;

    CommandSpec(Command command, String[] requiredArgs, String[] optionalArgs, String description)
    {
      this.command = command;
      this.requiredArgs = requiredArgs;
      this.optionalArgs = optionalArgs;
      this.description = description;
    }

  }

  StramCli()
  {
    globalCommands.put("help", new CommandSpec(new HelpCommand(), null, null, "Show help"));
    globalCommands.put("connect", new CommandSpec(new ConnectCommand(), new String[] {"app-id"}, null, "Connect to an app"));
    globalCommands.put("launch", new CommandSpec(new LaunchCommand(), new String[] {"jar-file"}, new String[] {"class-name"}, "Launch an app"));
    globalCommands.put("launch-local", new CommandSpec(new LaunchCommand(), new String[] {"jar-file"}, new String[] {"class-name"}, "Launch an app in local mode"));
    globalCommands.put("shutdown-app", new CommandSpec(new ShutdownAppCommand(), new String[] {"app-id"}, null, "Shutdown an app"));
    globalCommands.put("list-apps", new CommandSpec(new ListAppsCommand(), null, new String[] {"app-id"}, "List applications"));
    globalCommands.put("kill-app", new CommandSpec(new KillAppCommand(), new String[] {"app-id"}, null, "Kill an app"));
    globalCommands.put("show-logical-plan", new CommandSpec(new ShowLogicalPlanCommand(), new String[] {"app-id"}, null, "Show logical plan of an app class"));
    globalCommands.put("alias", new CommandSpec(new AliasCommand(), new String[] {"alias-name", "command"}, null, "Create a command alias"));
    globalCommands.put("source", new CommandSpec(new SourceCommand(), new String[] {"file"}, null, "Execute the commands in a file"));
    globalCommands.put("exit", new CommandSpec(new ExitCommand(), null, null, "Exit the CLI"));
    globalCommands.put("begin-macro", new CommandSpec(new BeginMacroCommand(), new String[] {"name"}, null, "Begin Macro Definition"));

    connectedCommands.put("list-containers", new CommandSpec(new ListContainersCommand(), null, null, "List containers"));
    connectedCommands.put("list-operators", new CommandSpec(new ListOperatorsCommand(), null, new String[] {"pattern"}, "List operators"));
    connectedCommands.put("kill-container", new CommandSpec(new KillContainerCommand(), new String[] {"container-id"}, null, "Kill a container"));
    connectedCommands.put("shutdown-app", new CommandSpec(new ShutdownAppCommand(), null, null, "Shutdown an app"));
    connectedCommands.put("kill-app", new CommandSpec(new KillAppCommand(), null, null, "Kill an app"));
    connectedCommands.put("wait", new CommandSpec(new WaitCommand(), new String[] {"timeout"}, null, "Wait for completion of current application"));
    connectedCommands.put("start-recording", new CommandSpec(new StartRecordingCommand(), new String[] {"operator-id"}, new String[] {"port-name"}, "Start recording"));
    connectedCommands.put("stop-recording", new CommandSpec(new StopRecordingCommand(), new String[] {"operator-id"}, new String[] {"port-name"}, "Stop recording"));
    connectedCommands.put("sync-recording", new CommandSpec(new SyncRecordingCommand(), new String[] {"operator-id"}, new String[] {"port-name"}, "Sync recording"));
    connectedCommands.put("set-operator-property", new CommandSpec(new SetOperatorPropertyLiveCommand(), new String[] {"operator-name", "property-name", "property-value"}, null, "Set a property of an operator"));
    connectedCommands.put("begin-logical-plan-change", new CommandSpec(new BeginLogicalPlanChangeCommand(), null, null, "Begin Logical Plan Change"));

    logicalPlanChangeCommands.put("help", new CommandSpec(new HelpCommand(), null, null, "Show help"));
    logicalPlanChangeCommands.put("create-operator", new CommandSpec(new CreateOperatorCommand(), new String[] {"operator-name", "class-name"}, null, "Create an operator"));
    logicalPlanChangeCommands.put("create-stream", new CommandSpec(new CreateStreamCommand(), new String[] {"stream-name", "from-operator-name", "from-port-name", "to-operator-name", "to-port-name"}, null, "Create a stream"));
    logicalPlanChangeCommands.put("remove-operator", new CommandSpec(new RemoveOperatorCommand(), new String[] {"operator-name"}, null, "Remove an operator"));
    logicalPlanChangeCommands.put("remove-stream", new CommandSpec(new RemoveStreamCommand(), new String[] {"stream-name"}, null, "Remove a stream"));
    logicalPlanChangeCommands.put("set-operator-property", new CommandSpec(new SetOperatorPropertyCommand(), new String[] {"operator-name", "property-name", "property-value"}, null, "Set a property of an operator"));
    logicalPlanChangeCommands.put("set-operator-attribute", new CommandSpec(new SetOperatorAttributeCommand(), new String[] {"operator-name", "attr-name", "attr-value"}, null, "Set an attribute of an operator"));
    logicalPlanChangeCommands.put("set-port-attribute", new CommandSpec(new SetPortAttributeCommand(), new String[] {"operator-name", "port-name", "attr-name", "attr-value"}, null, "Set an attribute of a port"));
    logicalPlanChangeCommands.put("set-stream-attribute", new CommandSpec(new SetStreamAttributeCommand(), new String[] {"stream-name", "attr-name", "attr-value"}, null, "Set an attribute of a stream"));
    logicalPlanChangeCommands.put("queue", new CommandSpec(new QueueCommand(), null, null, "Show the queue of the plan change"));
    logicalPlanChangeCommands.put("submit", new CommandSpec(new SubmitCommand(), null, null, "Submit the plan change"));
    logicalPlanChangeCommands.put("abort", new CommandSpec(new AbortCommand(), null, null, "Abort the plan change"));
    StramClientUtils.addStramResources(conf);

  }

  protected ApplicationReport getApplication(int appSeq)
  {
    List<ApplicationReport> appList = getApplicationList();
    for (ApplicationReport ar : appList) {
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

  public void init(String[] args) throws IOException
  {
    consolePresent = (System.console() != null);
    Options options = new Options();
    options.addOption("e", true, "Commands are read from the argument");
    CommandLineParser parser = new BasicParser();
    try {
      CommandLine cmd = parser.parse(options, args);
      if (cmd.hasOption("e")) {
        commandsToExecute = cmd.getOptionValues("e");
        consolePresent = false;
        for (String command : commandsToExecute) {
          LOG.debug("Command to be executed: {}", command);
        }
      }
    }
    catch (ParseException ex) {
      System.err.println("Invalid argument: " + ex);
      System.exit(1);
    }
    // Need to initialize security before starting RPC for the credentials to
    // take effect
    StramUserLogin.attemptAuthentication(conf);
    YarnClientHelper yarnClient = new YarnClientHelper(conf);
    rmClient = new ClientRMHelper(yarnClient);
  }

  /**
   * Why reinvent the wheel?
   * JLine 2.x supports search and more.. but it uses the same package as JLine 0.9.x
   * Hadoop bundles and forces 0.9.x through zookeeper into our class path (when CLI is launched via hadoop command).
   * And Jline 0.9.x hijacked Ctrl-R for REDISPLAY
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

  private void processSourceFile(String fileName, ConsoleReader reader) throws FileNotFoundException, IOException
  {
    BufferedReader br = new BufferedReader(new FileReader(fileName));
    String line;
    while ((line = br.readLine()) != null) {
      processLine(line, reader, true);
    }
    br.close();
  }

  private void setupCompleter(ConsoleReader reader)
  {
    List<Completor> completors = new LinkedList<Completor>();
    completors.add(new SimpleCompletor(connectedCommands.keySet().toArray(new String[] {})));
    completors.add(new SimpleCompletor(globalCommands.keySet().toArray(new String[] {})));
    completors.add(new SimpleCompletor(logicalPlanChangeCommands.keySet().toArray(new String[] {})));

    List<Completor> launchCompletors = new LinkedList<Completor>();
    launchCompletors.add(new SimpleCompletor(new String[] {"launch", "launch-local", "source"}));
    launchCompletors.add(new FileNameCompletor()); // jarFile
    launchCompletors.add(new FileNameCompletor()); // topology
    completors.add(new ArgumentCompletor(launchCompletors));

    reader.addCompletor(new MultiCompletor(completors));
  }

  private void setupHistory(ConsoleReader reader)
  {
    File historyFile = new File(StramClientUtils.getSettingsRootDir(), ".history");
    historyFile.getParentFile().mkdirs();
    try {
      History history = new History(historyFile);
      reader.setHistory(history);
    }
    catch (IOException ex) {
      System.err.printf("Unable to open %s for writing.", historyFile);
    }
  }

  public void run() throws IOException
  {
    ConsoleReader reader = new ConsoleReaderExt();
    reader.setBellEnabled(false);
    try {
      processSourceFile(System.getProperty("user.home") + "/.stram/clirc", reader);
    }
    catch (Exception ex) {
      // ignore
    }
    if (consolePresent) {
      printWelcomeMessage();
      setupCompleter(reader);
      setupHistory(reader);
    }
    String line;
    PrintWriter out = new PrintWriter(System.out);
    int i = 0;
    while (true) {
      if (commandsToExecute != null) {
        if (i >= commandsToExecute.length) {
          break;
        }
        line = commandsToExecute[i++];
      }
      else {
        line = readLine(reader, "");
        if (line == null) {
          break;
        }
      }
      processLine(line, reader, true);
      out.flush();
    }
    System.out.println("exit");
  }

  private List<String> expandMacro(List<String> lines, String[] args)
  {
    List<String> expandedLines = new ArrayList<String>();

    for (String line : lines) {
      int previousIndex = 0;
      String expandedLine = "";
      while (true) {
        int currentIndex = line.indexOf('$', previousIndex);
        if (currentIndex > 0 && line.length() > currentIndex + 1) {
          int argIndex = line.charAt(currentIndex + 1) - '0';
          if (args.length > argIndex && argIndex >= 0) {
            expandedLine += line.substring(previousIndex, currentIndex);
            expandedLine += args[argIndex];
          }
          else {
            expandedLine += line.substring(previousIndex, currentIndex + 2);
          }
          currentIndex += 2;
        }
        else {
          expandedLine += line.substring(previousIndex);
          expandedLines.add(expandedLine);
          break;
        }
        previousIndex = currentIndex;
      }
    }
    return expandedLines;
  }

  private void processLine(String line, ConsoleReader reader, boolean expandMacroAlias)
  {
    try {
      String[] commands = line.split("\\s*;\\s*");

      for (String command : commands) {
        String[] args = command.split("\\s+");
        if (StringUtils.isBlank(args[0])) {
          continue;
        }
        if (expandMacroAlias) {
          if (macros.containsKey(args[0])) {
            List<String> macroItems = expandMacro(macros.get(args[0]), args);
            for (String macroItem : macroItems) {
              System.out.println("expanded-macro> " + macroItem);
              processLine(macroItem, reader, false);
            }
            continue;
          }

          if (aliases.containsKey(args[0])) {
            args[0] = aliases.get(args[0]);
          }
        }
        CommandSpec cs = null;
        if (changingLogicalPlan) {
          cs = logicalPlanChangeCommands.get(args[0]);
        }
        else {
          if (currentApp != null) {
            cs = connectedCommands.get(args[0]);
          }
          if (cs == null) {
            cs = globalCommands.get(args[0]);
          }
        }
        if (cs == null) {
          System.err.println("Invalid command '" + args[0] + "'. Type \"help\" for list of commands");
        }
        else {
          int minArgs = 0;
          int maxArgs = 0;
          if (cs.requiredArgs != null) {
            minArgs = cs.requiredArgs.length;
            maxArgs = cs.requiredArgs.length;
          }
          if (cs.optionalArgs != null) {
            maxArgs += cs.optionalArgs.length;
          }
          if (args.length - 1 < minArgs || args.length - 1 > maxArgs) {
            System.err.print("Usage: " + args[0]);
            if (cs.requiredArgs != null) {
              for (String arg : cs.requiredArgs) {
                System.err.print(" <" + arg + ">");
              }
            }
            if (cs.optionalArgs != null) {
              for (String arg : cs.optionalArgs) {
                System.err.print(" [<" + arg + ">]");
              }
            }
            System.err.println();
          }
          else {
            cs.command.execute(args, reader);
          }
        }
      }
    }
    catch (CliException e) {
      System.err.println(e.getMessage());
      LOG.info("Error processing line: " + line, e);
    }
    catch (Exception e) {
      System.err.println("Unexpected error: " + e);
    }
  }

  private void printWelcomeMessage()
  {
    System.out.println("Stram CLI " + VersionInfo.getVersion() + " " + VersionInfo.getDate() + " " + VersionInfo.getRevision());
  }

  private void printHelp(Map<String, CommandSpec> commandSpecs)
  {
    for (Map.Entry<String, CommandSpec> entry : commandSpecs.entrySet()) {
      CommandSpec cs = entry.getValue();
      if (consolePresent) {
        System.out.print("\033[0;93m");
        System.out.print(entry.getKey());
        System.out.print("\033[0m");
      }
      else {
        System.out.print(entry.getKey());
      }
      if (cs.requiredArgs != null) {
        for (String arg : cs.requiredArgs) {
          if (consolePresent) {
            System.out.print(" \033[3m" + arg + "\033[0m");
          }
          else {
            System.out.print(" <" + arg + ">");
          }
        }
      }
      if (cs.optionalArgs != null) {
        for (String arg : cs.optionalArgs) {
          if (consolePresent) {
            System.out.print(" [\033[3m" + arg + "\033[0m]");
          }
          else {
            System.out.print(" [<" + arg + ">]");
          }
        }
      }
      System.out.println("\n\t" + cs.description);
      //System.out.println();
    }
  }

  private String readLine(ConsoleReader reader, String promptMessage)
          throws IOException
  {
    String prompt = "";
    if (consolePresent) {
      prompt = promptMessage + "\n";
      if (changingLogicalPlan) {
        prompt += "logical-plan-change";
      }
      else {
        prompt += "stramcli";
      }
      if (currentApp != null) {
        prompt += " (";
        prompt += currentApp.getApplicationId().toString();
        prompt += ") ";
      }
      prompt += "> ";
    }
    String line = reader.readLine(prompt);
    if (line == null) {
      return null;
    }
    return line.trim();
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
      throw new CliException("Application terminated.");
    }

    WebServicesClient wsClient = new WebServicesClient();
    Client client = wsClient.getClient();
    client.setFollowRedirects(true);
    WebResource r = client.resource("http://" + currentApp.getTrackingUrl()).path(StramWebServices.PATH).path(resourcePath);
    try {
      return wsClient.process(r, ClientResponse.class, new WebServicesClient.WebServicesHandler<ClientResponse>()
      {
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

  /*
   * Below is the implementation of all commands
   */
  private class HelpCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      System.out.println("GLOBAL COMMANDS EXCEPT WHEN CHANGING LOGICAL PLAN:\n");
      printHelp(globalCommands);
      System.out.println();
      System.out.println("COMMANDS WHEN CONNECTED TO AN APP (via connect <appid>) EXCEPT WHEN CHANGING LOGICAL PLAN:\n");
      printHelp(connectedCommands);
      System.out.println();
      System.out.println("COMMANDS WHEN CHANGING LOGICAL PLAN:\n");
      printHelp(logicalPlanChangeCommands);
      System.out.println();
    }

  }

  private class ConnectCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {

      currentApp = getApplication(Integer.parseInt(args[1]));
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

  }

  private class LaunchCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
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
            for (Completor c : completors) {
              reader.removeCompletor(c);
            }
            String optionLine = reader.readLine("Pick application? ");
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
            currentApp = rmClient.getApplicationReport(appId);
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

  }

  private class ShutdownAppCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
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
        currentApp = null;
      }
      catch (Exception e) {
        throw new CliException("Failed to request " + r.getURI(), e);
      }
    }

  }

  private class ListAppsCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
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

  }

  private class KillAppCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      if (args.length == 1) {
        if (currentApp == null) {
          throw new CliException("No application selected");
        }
        else {
          try {
            rmClient.killApplication(currentApp.getApplicationId());
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
            currentApp = null;
          }
        }
      }
      catch (YarnRemoteException e) {
        throw new CliException("Failed to kill " + ((app == null || app.getApplicationId() == null) ? "unknown application" : app.getApplicationId()) + ". Aborting killing of any additional applications.", e);
      }
      catch (NumberFormatException nfe) {
        throw new CliException("Invalid application Id " + args[i], nfe);
      }
      catch (NullPointerException npe) {
        throw new CliException("Application with Id " + args[i] + " does not seem to be alive!", npe);
      }

    }

  }

  private class AliasCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      if (args[1].equals(args[2])) {
        throw new CliException("Alias to itself!");
      }
      aliases.put(args[1], args[2]);
    }

  }

  private class SourceCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      processSourceFile(args[1], reader);
    }

  }

  private class ExitCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      System.exit(0);
    }

  }

  private class ListContainersCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      ClientResponse rsp = getResource(StramWebServices.PATH_CONTAINERS);
      JSONObject json = rsp.getEntity(JSONObject.class);
      if (args.length == 1) {
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
              for (int argc = args.length; argc-- > 1;) {
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

  }

  private class ListOperatorsCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      ClientResponse rsp = getResource(StramWebServices.PATH_OPERATORS);
      JSONObject json = rsp.getEntity(JSONObject.class);

      if (args.length > 1) {
        String singleKey = "" + json.keys().next();
        JSONArray matches = new JSONArray();
        // filter operators
        JSONArray arr = json.getJSONArray(singleKey);
        for (int i = 0; i < arr.length(); i++) {
          Object val = arr.get(i);
          if (val.toString().matches(args[1])) {
            matches.put(val);
          }
        }
        json.put(singleKey, matches);
      }

      System.out.println(json.toString(2));
    }

  }

  private class KillContainerCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
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

  }

  private class WaitCommand implements Command
  {
    @Override
    public void execute(String[] args, final ConsoleReader reader) throws Exception
    {
      if (currentApp == null) {
        throw new CliException("No application selected");
      }
      int timeout = Integer.valueOf(args[1]);

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

  }

  private class StartRecordingCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {

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

  }

  private class StopRecordingCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {

      WebServicesClient webServicesClient = new WebServicesClient();
      WebResource r = getPostResource(webServicesClient).path(StramWebServices.PATH_STOPRECORDING);
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
        System.out.println("stop recording requested: " + response);
      }
      catch (Exception e) {
        throw new CliException("Failed to request " + r.getURI(), e);
      }
    }

  }

  private class SyncRecordingCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {

      WebServicesClient webServicesClient = new WebServicesClient();
      WebResource r = getPostResource(webServicesClient).path(StramWebServices.PATH_SYNCRECORDING);
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
        System.out.println("sync recording requested: " + response);
      }
      catch (Exception e) {
        throw new CliException("Failed to request " + r.getURI(), e);
      }
    }

  }

  private class SetOperatorPropertyLiveCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      if (currentApp == null) {
        throw new CliException("No application selected");
      }
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

  }

  private class SetOperatorPropertyCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String operatorName = args[1];
      String propertyName = args[2];
      String propertyValue = args[3];
      SetOperatorPropertyRequest request = new SetOperatorPropertyRequest();
      request.setOperatorName(operatorName);
      request.setPropertyName(propertyName);
      request.setPropertyValue(propertyValue);
      logicalPlanRequestQueue.add(request);
    }

  }

  private class BeginLogicalPlanChangeCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      changingLogicalPlan = true;
    }

  }

  private class ShowLogicalPlanCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

  }

  private class CreateOperatorCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String operatorName = args[1];
      String className = args[2];
      CreateOperatorRequest request = new CreateOperatorRequest();
      request.setOperatorName(operatorName);
      request.setOperatorFQCN(className);
      logicalPlanRequestQueue.add(request);
    }

  }

  private class RemoveOperatorCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String operatorName = args[1];
      RemoveOperatorRequest request = new RemoveOperatorRequest();
      request.setOperatorName(operatorName);
      logicalPlanRequestQueue.add(request);
    }

  }

  private class CreateStreamCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
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
      logicalPlanRequestQueue.add(request);
    }

  }

  private class RemoveStreamCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String streamName = args[1];
      RemoveStreamRequest request = new RemoveStreamRequest();
      request.setStreamName(streamName);
      logicalPlanRequestQueue.add(request);
    }

  }

  private class SetOperatorAttributeCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String operatorName = args[1];
      String attributeName = args[2];
      String attributeValue = args[3];
      SetOperatorAttributeRequest request = new SetOperatorAttributeRequest();
      request.setOperatorName(operatorName);
      request.setAttributeName(attributeName);
      request.setAttributeValue(attributeValue);
      logicalPlanRequestQueue.add(request);
    }

  }

  private class SetStreamAttributeCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String streamName = args[1];
      String attributeName = args[2];
      String attributeValue = args[3];
      SetStreamAttributeRequest request = new SetStreamAttributeRequest();
      request.setStreamName(streamName);
      request.setAttributeName(attributeName);
      request.setAttributeValue(attributeValue);
      logicalPlanRequestQueue.add(request);
    }

  }

  private class SetPortAttributeCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String operatorName = args[1];
      String attributeName = args[2];
      String attributeValue = args[3];
      SetPortAttributeRequest request = new SetPortAttributeRequest();
      request.setOperatorName(operatorName);
      request.setAttributeName(attributeName);
      request.setAttributeValue(attributeValue);
      logicalPlanRequestQueue.add(request);
    }

  }

  private class AbortCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      logicalPlanRequestQueue.clear();
      changingLogicalPlan = false;
    }

  }

  private class SubmitCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      if (logicalPlanRequestQueue.isEmpty()) {
        throw new CliException("Nothing to submit. Type \"abort\" to abort change");
      }
      WebServicesClient webServicesClient = new WebServicesClient();
      WebResource r = getPostResource(webServicesClient).path(StramWebServices.PATH_LOGICAL_PLAN_MODIFICATION);
      try {
        final Map<String, Object> m = new HashMap<String, Object>();
        ObjectMapper mapper = new ObjectMapper();
        m.put("requests", logicalPlanRequestQueue);
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
      logicalPlanRequestQueue.clear();
      changingLogicalPlan = false;
    }

  }

  private class QueueCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      ObjectMapper mapper = new ObjectMapper();
      System.out.println(mapper.defaultPrettyPrintingWriter().writeValueAsString(logicalPlanRequestQueue));
      System.out.println("Total operations in queue: " + logicalPlanRequestQueue.size());
    }

  }

  private class BeginMacroCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String name = args[1];
      if (macros.containsKey(name) || aliases.containsKey(name)) {
        System.err.println("Name '" + name + "' already exists.");
        return;
      }
      try {
        List<String> commands = new ArrayList<String>();
        while (true) {
          String line = reader.readLine("macro def (" + name + ") > ");
          if (line.equals("end")) {
            macros.put(name, commands);
            System.out.println("Macro '" + name + "' created.");
            return;
          }
          else if (line.equals("abort")) {
            System.err.println("Aborted");
            return;
          }
          else {
            commands.add(line);
          }
        }
      }
      catch (IOException ex) {
        System.err.println("Aborted");
      }
    }

  }

  public static void main(String[] args) throws Exception
  {
    StramCli shell = new StramCli();
    shell.init(args);
    shell.run();
  }

}
