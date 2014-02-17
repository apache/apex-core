/**
 * Copyright (c) 2012-2013 DataTorrent, Inc. All rights reserved.
 */
package com.datatorrent.stram.cli;

import java.io.*;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;

import com.datatorrent.stram.license.*;
import com.datatorrent.stram.license.agent.protocol.LicensingAgentProtocolHelper;
import jline.console.ConsoleReader;
import jline.console.completer.*;
import jline.console.history.FileHistory;
import jline.console.history.History;
import jline.console.history.MemoryHistory;

import javax.ws.rs.core.MediaType;

import com.google.common.collect.Sets;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.cli.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.tools.ant.DirectoryScanner;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAGContext;

import com.datatorrent.stram.StramClient;
import com.datatorrent.stram.client.RecordingsAgent;
import com.datatorrent.stram.client.RecordingsAgent.RecordingInfo;
import com.datatorrent.stram.client.StramAgent;
import com.datatorrent.stram.client.StramAppLauncher;
import com.datatorrent.stram.client.StramAppLauncher.AppFactory;
import com.datatorrent.stram.client.StramClientUtils;
import com.datatorrent.stram.client.StramClientUtils.ClientRMHelper;
import com.datatorrent.stram.client.StramClientUtils.YarnClientHelper;
import com.datatorrent.stram.client.WebServicesVersionConversion.IncompatibleVersionException;
import com.datatorrent.stram.codec.LogicalPlanSerializer;
import com.datatorrent.stram.license.util.Util;
import com.datatorrent.stram.plan.logical.*;
import com.datatorrent.stram.security.StramUserLogin;
import com.datatorrent.stram.util.VersionInfo;
import com.datatorrent.stram.util.WebServicesClient;
import com.datatorrent.stram.webapp.StramWebServices;

/**
 *
 * Provides command line interface for a streaming application on hadoop (yarn)<p>
 *
 * @since 0.3.2
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public class DTCli
{
  private static final Logger LOG = LoggerFactory.getLogger(DTCli.class);
  private static final long TIMEOUT_AFTER_ACTIVATE_LICENSE = 10000;
  private final Configuration conf = new YarnConfiguration();
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
  private final List<LogicalPlanRequest> logicalPlanRequestQueue = new ArrayList<LogicalPlanRequest>();
  private FileHistory topLevelHistory;
  private FileHistory changingLogicalPlanHistory;
  private String jsonp;
  private boolean raw = false;
  private RecordingsAgent recordingsAgent;
  private final ObjectMapper mapper = new ObjectMapper();
  private String pagerCommand;
  private Process pagerProcess;
  private int verboseLevel = 0;
  private final Tokenizer tokenizer = new Tokenizer();
  private final Map<String, String> variableMap = new HashMap<String, String>();
  private static boolean lastCommandError = false;

  private static class FileLineReader extends ConsoleReader
  {
    private final BufferedReader br;

    FileLineReader(String fileName) throws IOException
    {
      super();
      fileName = expandFileName(fileName, true);
      br = new BufferedReader(new FileReader(fileName));
    }

    @Override
    public String readLine(String prompt) throws IOException
    {
      return br.readLine();
    }

    public void close() throws IOException
    {
      br.close();
    }

  }

  public class Tokenizer
  {
    private void appendToCommandBuffer(List<String> commandBuffer, StringBuffer buf, boolean potentialEmptyArg)
    {
      if (potentialEmptyArg || buf.length() > 0) {
        commandBuffer.add(buf.toString());
        buf.setLength(0);
      }
    }

    private List<String> startNewCommand(LinkedList<List<String>> resultBuffer)
    {
      List<String> newCommand = new ArrayList<String>();
      if (!resultBuffer.isEmpty()) {
        List<String> lastCommand = resultBuffer.peekLast();
        if (lastCommand.size() == 1) {
          String first = lastCommand.get(0);
          if (first.matches("^[A-Za-z][A-Za-z0-9]*=.*")) {
            // This is a variable assignment
            int equalSign = first.indexOf('=');
            variableMap.put(first.substring(0, equalSign), first.substring(equalSign + 1));
            resultBuffer.removeLast();
          }
        }
      }
      resultBuffer.add(newCommand);
      return newCommand;
    }

    public List<String[]> tokenize(String commandLine)
    {
      LinkedList<List<String>> resultBuffer = new LinkedList<List<String>>();
      List<String> commandBuffer = startNewCommand(resultBuffer);

      if (commandLine != null) {
        commandLine = ltrim(commandLine);
        if (commandLine.startsWith("#")) {
          return null;
        }

        int len = commandLine.length();
        boolean insideQuotes = false;
        boolean potentialEmptyArg = false;
        StringBuffer buf = new StringBuffer(commandLine.length());

        for (@SuppressWarnings("AssignmentToForLoopParameter") int i = 0; i < len; ++i) {
          char c = commandLine.charAt(i);
          if (c == '"') {
            potentialEmptyArg = true;
            insideQuotes = !insideQuotes;
          }
          else if (c == '\\') {
            if (len > i + 1) {
              switch (commandLine.charAt(i + 1)) {
                case 'n':
                  buf.append("\n");
                  break;
                case 't':
                  buf.append("\t");
                  break;
                case 'r':
                  buf.append("\r");
                  break;
                case 'b':
                  buf.append("\b");
                  break;
                case 'f':
                  buf.append("\f");
                  break;
                default:
                  buf.append(commandLine.charAt(i + 1));
              }
              ++i;
            }
          }
          else {
            if (insideQuotes) {
              buf.append(c);
            }
            else {

              if (c == '$') {
                StringBuilder variableName = new StringBuilder(32);
                if (len > i + 1) {
                  if (commandLine.charAt(i + 1) == '{') {
                    ++i;
                    while (len > i + 1) {
                      char ch = commandLine.charAt(i + 1);
                      if (ch != '}') {
                        variableName.append(ch);
                      }
                      ++i;
                      if (ch == '}') {
                        break;
                      }
                      if (len <= i + 1) {
                        throw new CliException("Parse error: unmatched brace");
                      }
                    }
                  }
                  else {
                    while (len > i + 1) {
                      char ch = commandLine.charAt(i + 1);
                      if ((variableName.length() > 0 && ch >= '0' && ch <= '9') || ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z'))) {
                        variableName.append(ch);
                      }
                      else {
                        break;
                      }
                      ++i;
                    }
                  }
                  if (variableName.length() == 0) {
                    buf.append(c);
                  }
                  else {
                    String value = variableMap.get(variableName.toString());
                    if (value != null) {
                      buf.append(value);
                    }
                  }
                }
                else {
                  buf.append(c);
                }
              }
              else if (c == ';') {
                appendToCommandBuffer(commandBuffer, buf, potentialEmptyArg);
                commandBuffer = startNewCommand(resultBuffer);
              }
              else if (Character.isWhitespace(c)) {
                appendToCommandBuffer(commandBuffer, buf, potentialEmptyArg);
                potentialEmptyArg = false;
                if (len > i + 1 && commandLine.charAt(i + 1) == '#') {
                  break;
                }
              }
              else {
                buf.append(c);
              }
            }
          }
        }
        appendToCommandBuffer(commandBuffer, buf, potentialEmptyArg);
      }
      startNewCommand(resultBuffer);
      List<String[]> result = new ArrayList<String[]>();
      for (List<String> command : resultBuffer) {
        String[] commandArray = new String[command.size()];
        result.add(command.toArray(commandArray));
      }
      return result;
    }

  }

  private interface Command
  {
    void execute(String[] args, ConsoleReader reader) throws Exception;

  }

  private static class Arg
  {
    final String name;

    Arg(String name)
    {
      this.name = name;
    }

    @Override
    public String toString()
    {
      return name;
    }

  }

  private static class FileArg extends Arg
  {
    FileArg(String name)
    {
      super(name);
    }

  }

  private static class CommandArg extends Arg
  {
    CommandArg(String name)
    {
      super(name);
    }

  }

  private StramAppLauncher getStramAppLauncher(String jarfileUri, Configuration config, boolean ignorePom) throws Exception
  {
    URI uri = new URI(jarfileUri);
    String scheme = uri.getScheme();
    StramAppLauncher appLauncher = null;
    if (scheme == null || scheme.equals("file")) {
      File jf = new File(uri.getPath());
      appLauncher = new StramAppLauncher(jf, config, ignorePom);
    }
    else if (scheme.equals("hdfs")) {
      FileSystem fs = FileSystem.get(uri, conf);
      Path path = new Path(uri.getPath());
      appLauncher = new StramAppLauncher(fs, path, config, ignorePom);
    }
    if (appLauncher != null) {
      if (verboseLevel > 0) {
        System.err.print(appLauncher.getMvnBuildClasspathOutput());
      }
      return appLauncher;
    }
    else {
      throw new CliException("Scheme " + scheme + " not supported.");
    }
  }

  private static class CommandSpec
  {
    Command command;
    Arg[] requiredArgs;
    Arg[] optionalArgs;
    String description;

    CommandSpec(Command command, Arg[] requiredArgs, Arg[] optionalArgs, String description)
    {
      this.command = command;
      this.requiredArgs = requiredArgs;
      this.optionalArgs = optionalArgs;
      this.description = description;
    }

    void verifyArguments(String[] args) throws CliException
    {
      int minArgs = 0;
      int maxArgs = 0;
      if (requiredArgs != null) {
        minArgs = requiredArgs.length;
        maxArgs = requiredArgs.length;
      }
      if (optionalArgs != null) {
        maxArgs += optionalArgs.length;
      }
      if (args.length - 1 < minArgs || args.length - 1 > maxArgs) {
        throw new CliException("Command parameter error");
      }
    }

    void printUsage(String cmd)
    {
      System.err.print("Usage: " + cmd);
      if (requiredArgs != null) {
        for (Arg arg : requiredArgs) {
          System.err.print(" <" + arg + ">");
        }
      }
      if (optionalArgs != null) {
        for (Arg arg : optionalArgs) {
          System.err.print(" [<" + arg + ">]");
        }
      }
      System.err.println();
    }

  }

  private static class OptionsCommandSpec extends CommandSpec
  {
    Options options;

    OptionsCommandSpec(Command command, Arg[] requiredArgs, Arg[] optionalArgs, String description, Options options)
    {
      super(command, requiredArgs, optionalArgs, description);
      this.options = options;
    }

    @Override
    void verifyArguments(String[] args) throws CliException
    {
      try {
        args = new PosixParser().parse(options, args).getArgs();
        super.verifyArguments(args);
      }
      catch (Exception ex) {
        throw new CliException("Command parameter error");
      }
    }

    @Override
    void printUsage(String cmd)
    {
      super.printUsage(cmd + ((options == null) ? "" : " [options]"));
      if (options != null) {
        System.out.println("Options:");
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter pw = new PrintWriter(System.out);
        formatter.printOptions(pw, 80, options, 4, 4);
        pw.flush();
      }
    }

  }

  DTCli()
  {
    //
    // Global command specification starts here
    //
    globalCommands.put("help", new CommandSpec(new HelpCommand(),
                                               null,
                                               new Arg[] {new CommandArg("command")},
                                               "Show help"));
    globalCommands.put("connect", new CommandSpec(new ConnectCommand(),
                                                  new Arg[] {new Arg("app-id")},
                                                  null,
                                                  "Connect to an app"));
    globalCommands.put("launch", new OptionsCommandSpec(new LaunchCommand(),
                                                        new Arg[] {new FileArg("jar-file")},
                                                        new Arg[] {new Arg("class-name/property-file")},
                                                        "Launch an app", getLaunchCommandLineOptions()));
    globalCommands.put("shutdown-app", new CommandSpec(new ShutdownAppCommand(),
                                                       new Arg[] {new Arg("app-id")},
                                                       null,
                                                       "Shutdown an app"));
    globalCommands.put("list-apps", new CommandSpec(new ListAppsCommand(),
                                                    null,
                                                    new Arg[] {new Arg("pattern")},
                                                    "List applications"));
    globalCommands.put("kill-app", new CommandSpec(new KillAppCommand(),
                                                   new Arg[] {new Arg("app-id")},
                                                   null,
                                                   "Kill an app"));
    globalCommands.put("show-logical-plan", new OptionsCommandSpec(new ShowLogicalPlanCommand(),
                                                                   new Arg[] {new FileArg("jar-file")},
                                                                   new Arg[] {new Arg("class-name")},
                                                                   "List apps in a jar or show logical plan of an app class",
                                                                   getShowLogicalPlanCommandLineOptions()));
    globalCommands.put("alias", new CommandSpec(new AliasCommand(),
                                                new Arg[] {new Arg("alias-name"), new CommandArg("command")},
                                                null,
                                                "Create a command alias"));
    globalCommands.put("source", new CommandSpec(new SourceCommand(),
                                                 new Arg[] {new FileArg("file")},
                                                 null,
                                                 "Execute the commands in a file"));
    globalCommands.put("exit", new CommandSpec(new ExitCommand(),
                                               null,
                                               null,
                                               "Exit the CLI"));
    globalCommands.put("begin-macro", new CommandSpec(new BeginMacroCommand(),
                                                      new Arg[] {new Arg("name")},
                                                      null,
                                                      "Begin Macro Definition ($1...$9 to access parameters and type 'end' to end the definition)"));
    globalCommands.put("dump-properties-file", new CommandSpec(new DumpPropertiesFileCommand(),
                                                               new Arg[] {new FileArg("out-file"), new FileArg("jar-file"), new Arg("class-name")},
                                                               null,
                                                               "Dump the properties file of an app class"));
    globalCommands.put("get-app-info", new CommandSpec(new GetAppInfoCommand(),
                                                       new Arg[] {new Arg("app-id")},
                                                       null,
                                                       "Get the information of an app"));
    globalCommands.put("set-pager", new CommandSpec(new SetPagerCommand(),
                                                    new Arg[] {new Arg("on/off")},
                                                    null,
                                                    "Set the pager program for output"));
    globalCommands.put("generate-license-request", new CommandSpec(new GenerateLicenseRequestCommand(),
                                                                   null,
                                                                   null,
                                                                   "Generate license request"));
    globalCommands.put("activate-license", new CommandSpec(new ActivateLicenseCommand(),
                                                           null,
                                                           new Arg[] {new FileArg("license-file")},
                                                           "Launch the license agent"));
    globalCommands.put("deactivate-license", new CommandSpec(new DeactivateLicenseCommand(),
                                                             null,
                                                             new Arg[] {new Arg("license-id")},
                                                             "Stop the license agent"));
    globalCommands.put("list-license-agents", new CommandSpec(new ListLicenseAgentsCommand(),
                                                              null,
                                                              null,
                                                              "Show IDs of all license agents"));
    globalCommands.put("show-license-status", new CommandSpec(new ShowLicenseStatusCommand(),
                                                              null,
                                                              new Arg[] {new FileArg("license-file")},
                                                              "Show the status of the license"));

    //
    // Connected command specification starts here
    //

    connectedCommands.put("list-containers", new CommandSpec(new ListContainersCommand(),
                                                             null,
                                                             null,
                                                             "List containers"));
    connectedCommands.put("list-operators", new CommandSpec(new ListOperatorsCommand(),
                                                            null,
                                                            new Arg[] {new Arg("pattern")},
                                                            "List operators"));
    connectedCommands.put("show-physical-plan", new CommandSpec(new ShowPhysicalPlanCommand(),
                                                                null,
                                                                null,
                                                                "Show physical plan"));
    connectedCommands.put("kill-container", new CommandSpec(new KillContainerCommand(),
                                                            new Arg[] {new Arg("container-id")},
                                                            null,
                                                            "Kill a container"));
    connectedCommands.put("shutdown-app", new CommandSpec(new ShutdownAppCommand(),
                                                          null,
                                                          new Arg[] {new Arg("app-id")},
                                                          "Shutdown an app"));
    connectedCommands.put("kill-app", new CommandSpec(new KillAppCommand(),
                                                      null,
                                                      new Arg[] {new Arg("app-id")},
                                                      "Kill an app"));
    connectedCommands.put("wait", new CommandSpec(new WaitCommand(),
                                                  new Arg[] {new Arg("timeout")},
                                                  null,
                                                  "Wait for completion of current application"));
    connectedCommands.put("start-recording", new CommandSpec(new StartRecordingCommand(),
                                                             new Arg[] {new Arg("operator-id")},
                                                             new Arg[] {new Arg("port-name")},
                                                             "Start recording"));
    connectedCommands.put("stop-recording", new CommandSpec(new StopRecordingCommand(),
                                                            new Arg[] {new Arg("operator-id")},
                                                            new Arg[] {new Arg("port-name")},
                                                            "Stop recording"));
    connectedCommands.put("get-operator-attributes", new CommandSpec(new GetOperatorAttributesCommand(),
                                                                     new Arg[] {new Arg("operator-name")},
                                                                     new Arg[] {new Arg("attribute-name")},
                                                                     "Get attributes of an operator"));
    connectedCommands.put("get-operator-properties", new CommandSpec(new GetOperatorPropertiesCommand(),
                                                                     new Arg[] {new Arg("operator-name")},
                                                                     new Arg[] {new Arg("property-name")},
                                                                     "Get properties of an operator"));
    connectedCommands.put("get-physical-operator-properties", new CommandSpec(new GetPhysicalOperatorPropertiesCommand(),
                                                                              new Arg[] {new Arg("operator-name")},
                                                                              new Arg[] {new Arg("property-name")},
                                                                              "Get properties of an operator"));

    connectedCommands.put("set-operator-property", new CommandSpec(new SetOperatorPropertyCommand(),
                                                                   new Arg[] {new Arg("operator-name"), new Arg("property-name"), new Arg("property-value")},
                                                                   null,
                                                                   "Set a property of an operator"));
    connectedCommands.put("set-physical-operator-property", new CommandSpec(new SetPhysicalOperatorPropertyCommand(),
                                                                            new Arg[] {new Arg("operator-id"), new Arg("property-name"), new Arg("property-value")},
                                                                            null,
                                                                            "Set a property of an operator"));
    connectedCommands.put("get-app-attributes", new CommandSpec(new GetAppAttributesCommand(),
                                                                null,
                                                                new Arg[] {new Arg("attribute-name")},
                                                                "Get attributes of the connected app"));
    connectedCommands.put("get-port-attributes", new CommandSpec(new GetPortAttributesCommand(),
                                                                 new Arg[] {new Arg("operator-name"), new Arg("port-name")},
                                                                 new Arg[] {new Arg("attribute-name")},
                                                                 "Get attributes of a port"));
    connectedCommands.put("begin-logical-plan-change", new CommandSpec(new BeginLogicalPlanChangeCommand(),
                                                                       null,
                                                                       null,
                                                                       "Begin Logical Plan Change"));
    connectedCommands.put("show-logical-plan", new OptionsCommandSpec(new ShowLogicalPlanCommand(),
                                                                      null,
                                                                      new Arg[] {new FileArg("jar-file"), new Arg("class-name")},
                                                                      "Show logical plan of an app class",
                                                                      getShowLogicalPlanCommandLineOptions()));
    connectedCommands.put("dump-properties-file", new CommandSpec(new DumpPropertiesFileCommand(),
                                                                  new Arg[] {new FileArg("out-file")},
                                                                  new Arg[] {new FileArg("jar-file"), new Arg("class-name")},
                                                                  "Dump the properties file of an app class"));
    connectedCommands.put("get-app-info", new CommandSpec(new GetAppInfoCommand(),
                                                          null,
                                                          new Arg[] {new Arg("app-id")},
                                                          "Get the information of an app"));
    connectedCommands.put("create-alert", new CommandSpec(new CreateAlertCommand(),
                                                          new Arg[] {new Arg("name"), new FileArg("file")},
                                                          null,
                                                          "Create an alert with the name and the given file that contains the spec"));
    connectedCommands.put("delete-alert", new CommandSpec(new DeleteAlertCommand(),
                                                          new Arg[] {new Arg("name")},
                                                          null,
                                                          "Delete an alert with the given name"));
    connectedCommands.put("list-alerts", new CommandSpec(new ListAlertsCommand(),
                                                         null,
                                                         null,
                                                         "List all alerts"));
    connectedCommands.put("get-recording-info", new CommandSpec(new GetRecordingInfoCommand(),
                                                                null,
                                                                new Arg[] {new Arg("operator-id"), new Arg("start-time")},
                                                                "Get tuple recording info"));

    //
    // Logical plan change command specification starts here
    //
    logicalPlanChangeCommands.put("help", new CommandSpec(new HelpCommand(),
                                                          null,
                                                          new Arg[] {new Arg("command")},
                                                          "Show help"));
    logicalPlanChangeCommands.put("create-operator", new CommandSpec(new CreateOperatorCommand(),
                                                                     new Arg[] {new Arg("operator-name"), new Arg("class-name")},
                                                                     null,
                                                                     "Create an operator"));
    logicalPlanChangeCommands.put("create-stream", new CommandSpec(new CreateStreamCommand(),
                                                                   new Arg[] {new Arg("stream-name"), new Arg("from-operator-name"), new Arg("from-port-name"), new Arg("to-operator-name"), new Arg("to-port-name")},
                                                                   null,
                                                                   "Create a stream"));
    logicalPlanChangeCommands.put("add-stream-sink", new CommandSpec(new AddStreamSinkCommand(),
                                                                     new Arg[] {new Arg("stream-name"), new Arg("to-operator-name"), new Arg("to-port-name")},
                                                                     null,
                                                                     "Add a sink to an existing stream"));
    logicalPlanChangeCommands.put("remove-operator", new CommandSpec(new RemoveOperatorCommand(),
                                                                     new Arg[] {new Arg("operator-name")},
                                                                     null,
                                                                     "Remove an operator"));
    logicalPlanChangeCommands.put("remove-stream", new CommandSpec(new RemoveStreamCommand(),
                                                                   new Arg[] {new Arg("stream-name")},
                                                                   null,
                                                                   "Remove a stream"));
    logicalPlanChangeCommands.put("set-operator-property", new CommandSpec(new SetOperatorPropertyCommand(),
                                                                           new Arg[] {new Arg("operator-name"), new Arg("property-name"), new Arg("property-value")},
                                                                           null,
                                                                           "Set a property of an operator"));
    logicalPlanChangeCommands.put("set-operator-attribute", new CommandSpec(new SetOperatorAttributeCommand(),
                                                                            new Arg[] {new Arg("operator-name"), new Arg("attr-name"), new Arg("attr-value")},
                                                                            null,
                                                                            "Set an attribute of an operator"));
    logicalPlanChangeCommands.put("set-port-attribute", new CommandSpec(new SetPortAttributeCommand(),
                                                                        new Arg[] {new Arg("operator-name"), new Arg("port-name"), new Arg("attr-name"), new Arg("attr-value")},
                                                                        null,
                                                                        "Set an attribute of a port"));
    logicalPlanChangeCommands.put("set-stream-attribute", new CommandSpec(new SetStreamAttributeCommand(),
                                                                          new Arg[] {new Arg("stream-name"), new Arg("attr-name"), new Arg("attr-value")},
                                                                          null,
                                                                          "Set an attribute of a stream"));
    logicalPlanChangeCommands.put("show-queue", new CommandSpec(new ShowQueueCommand(),
                                                                null,
                                                                null,
                                                                "Show the queue of the plan change"));
    logicalPlanChangeCommands.put("submit", new CommandSpec(new SubmitCommand(),
                                                            null,
                                                            null,
                                                            "Submit the plan change"));
    logicalPlanChangeCommands.put("abort", new CommandSpec(new AbortCommand(),
                                                           null,
                                                           null,
                                                           "Abort the plan change"));
  }

  private void printJson(String json) throws IOException
  {
    PrintStream os = getOutputPrintStream();

    if (jsonp != null) {
      os.println(jsonp + "(" + json + ");");
    }
    else {
      os.println(json);
    }
    os.flush();
    closeOutputPrintStream(os);
  }

  private void printJson(JSONObject json) throws JSONException, IOException
  {
    printJson(raw ? json.toString() : json.toString(2));
  }

  private void printJson(JSONArray jsonArray, String name) throws JSONException, IOException
  {
    JSONObject json = new JSONObject();
    json.put(name, jsonArray);
    printJson(json);
  }

  private <K, V> void printJson(Map<K, V> map) throws IOException, JSONException
  {
    printJson(new JSONObject(mapper.writeValueAsString(map)));
  }

  private <T> void printJson(List<T> list, String name) throws IOException, JSONException
  {
    printJson(new JSONArray(mapper.writeValueAsString(list)), name);
  }

  private PrintStream getOutputPrintStream() throws IOException
  {
    if (pagerCommand == null) {
      pagerProcess = null;
      return System.out;
    }
    else {
      pagerProcess = Runtime.getRuntime().exec(new String[] {"sh", "-c",
                                                             pagerCommand + " >/dev/tty"});
      return new PrintStream(pagerProcess.getOutputStream());
    }
  }

  private void closeOutputPrintStream(PrintStream os)
  {
    if (os != System.out) {
      os.close();
      try {
        pagerProcess.waitFor();
      }
      catch (InterruptedException ex) {
        LOG.debug("Interrupted");
      }
    }
  }

  private static String expandFileName(String fileName, boolean expandWildCard) throws IOException
  {
    if (fileName.matches("^[a-zA-Z]+:.*")) {
      // it's a URL
      return fileName;
    }

    // TODO: need to work with other users' home directory
    if (fileName.startsWith("~" + File.separator)) {
      fileName = System.getProperty("user.home") + fileName.substring(1);
    }
    fileName = new File(fileName).getCanonicalPath();
    //LOG.debug("Canonical path: {}", fileName);
    if (expandWildCard) {
      DirectoryScanner scanner = new DirectoryScanner();
      scanner.setIncludes(new String[] {fileName});
      scanner.scan();
      String[] files = scanner.getIncludedFiles();

      if (files.length == 0) {
        throw new CliException(fileName + " does not match any file");
      }
      else if (files.length > 1) {
        throw new CliException(fileName + " matches more than one file");
      }
      return files[0];
    }
    else {
      return fileName;
    }
  }

  private static String[] expandFileNames(String fileName) throws IOException
  {
    // TODO: need to work with other users
    if (fileName.matches("^[a-zA-Z]+:.*")) {
      // it's a URL
      return new String[] {fileName};
    }
    if (fileName.startsWith("~" + File.separator)) {
      fileName = System.getProperty("user.home") + fileName.substring(1);
    }
    fileName = new File(fileName).getCanonicalPath();
    LOG.debug("Canonical path: {}", fileName);
    DirectoryScanner scanner = new DirectoryScanner();
    scanner.setIncludes(new String[] {fileName});
    scanner.scan();
    return scanner.getIncludedFiles();
  }

  private static String expandCommaSeparatedFiles(String filenames) throws IOException
  {
    String[] entries = filenames.split(",");
    StringBuilder result = new StringBuilder(filenames.length());
    for (String entry : entries) {
      for (String file : expandFileNames(entry)) {
        if (result.length() > 0) {
          result.append(",");
        }
        result.append(file);
      }
    }
    return result.toString();
  }

  protected ApplicationReport getApplication(String appId)
  {
    List<ApplicationReport> appList = getApplicationList();
    if (StringUtils.isNumeric(appId)) {
      int appSeq = Integer.parseInt(appId);
      for (ApplicationReport ar : appList) {
        if (ar.getApplicationId().getId() == appSeq) {
          return ar;
        }
      }
    }
    else {
      for (ApplicationReport ar : appList) {
        if (ar.getApplicationId().toString().equals(appId)) {
          return ar;
        }
      }
    }
    return null;
  }

  private static class CliException extends RuntimeException
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
    options.addOption("v", false, "Verbose mode level 1");
    options.addOption("vv", false, "Verbose mode level 2");
    options.addOption("vvv", false, "Verbose mode level 3");
    options.addOption("vvvv", false, "Verbose mode level 4");
    options.addOption("r", false, "JSON Raw mode");
    options.addOption("p", true, "JSONP padding function");
    options.addOption("h", false, "Print this help");
    CommandLineParser parser = new BasicParser();
    try {
      CommandLine cmd = parser.parse(options, args);
      if (cmd.hasOption("v")) {
        verboseLevel = 1;
      }
      if (cmd.hasOption("vv")) {
        verboseLevel = 2;
      }
      if (cmd.hasOption("vvv")) {
        verboseLevel = 3;
      }
      if (cmd.hasOption("vvvv")) {
        verboseLevel = 4;
      }
      if (cmd.hasOption("r")) {
        raw = true;
      }
      if (cmd.hasOption("e")) {
        commandsToExecute = cmd.getOptionValues("e");
        consolePresent = false;
      }
      if (cmd.hasOption("p")) {
        jsonp = cmd.getOptionValue("p");
      }
      if (cmd.hasOption("h")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(DTCli.class.getSimpleName(), options);
        System.exit(0);
      }
    }
    catch (ParseException ex) {
      System.err.println("Invalid argument: " + ex);
      System.exit(1);
    }

    Level logLevel;
    switch (verboseLevel) {
      case 0:
        logLevel = Level.OFF;
        break;
      case 1:
        logLevel = Level.ERROR;
        break;
      case 2:
        logLevel = Level.WARN;
        break;
      case 3:
        logLevel = Level.INFO;
        break;
      default:
        logLevel = Level.DEBUG;
        break;
    }

    for (org.apache.log4j.Logger logger : new org.apache.log4j.Logger[] {org.apache.log4j.Logger.getRootLogger(),
                                                                         org.apache.log4j.Logger.getLogger(DTCli.class)}) {
      @SuppressWarnings("unchecked")
      Enumeration<Appender> allAppenders = logger.getAllAppenders();
      while (allAppenders.hasMoreElements()) {
        Appender appender = allAppenders.nextElement();
        if (appender instanceof ConsoleAppender) {
          ((ConsoleAppender)appender).setThreshold(logLevel);
        }
      }
    }

    if (commandsToExecute != null) {
      for (String command : commandsToExecute) {
        LOG.debug("Command to be executed: {}", command);
      }
    }
    StramClientUtils.addStramResources(conf);
    StramAgent.setResourceManagerWebappAddress(conf.get(YarnConfiguration.RM_WEBAPP_ADDRESS, "localhost:8088"));

    // Need to initialize security before starting RPC for the credentials to
    // take effect
    StramUserLogin.attemptAuthentication(conf);
    YarnClientHelper yarnClient = new YarnClientHelper(conf);
    rmClient = new ClientRMHelper(yarnClient);
    String socks = conf.get(CommonConfigurationKeysPublic.HADOOP_SOCKS_SERVER_KEY);
    if (socks != null) {
      int colon = socks.indexOf(':');
      if (colon > 0) {
        System.setProperty("socksProxyHost", socks.substring(0, colon));
        System.setProperty("socksProxyPort", socks.substring(colon + 1));
      }
    }
  }

  private void processSourceFile(String fileName, ConsoleReader reader) throws FileNotFoundException, IOException
  {
    boolean consolePresentSaved = consolePresent;
    consolePresent = false;
    FileLineReader fr = null;
    String line;
    try {
      fr = new FileLineReader(fileName);
      while ((line = fr.readLine("")) != null) {
        processLine(line, fr, true);
      }
    }
    finally {
      consolePresent = consolePresentSaved;
      if (fr != null) {
        fr.close();
      }
    }
  }

  private final static class MyNullCompleter implements Completer
  {
    public static final MyNullCompleter INSTANCE = new MyNullCompleter();

    @Override
    public int complete(final String buffer, final int cursor, final List<CharSequence> candidates)
    {
      candidates.add("");
      return cursor;
    }

  }

  private final static class MyFileNameCompleter extends FileNameCompleter
  {
    @Override
    public int complete(final String buffer, final int cursor, final List<CharSequence> candidates)
    {
      int result = super.complete(buffer, cursor, candidates);
      if (candidates.isEmpty()) {
        candidates.add("");
        result = cursor;
      }
      return result;
    }

  }

  private List<Completer> defaultCompleters()
  {
    Map<String, CommandSpec> commands = new TreeMap<String, CommandSpec>();

    commands.putAll(logicalPlanChangeCommands);
    commands.putAll(connectedCommands);
    commands.putAll(globalCommands);

    List<Completer> completers = new LinkedList<Completer>();
    for (Map.Entry<String, CommandSpec> entry : commands.entrySet()) {
      String command = entry.getKey();
      CommandSpec cs = entry.getValue();
      List<Completer> argCompleters = new LinkedList<Completer>();
      argCompleters.add(new StringsCompleter(command));
      Arg[] args = (Arg[])ArrayUtils.addAll(cs.requiredArgs, cs.optionalArgs);
      if (args != null) {
        if (cs instanceof OptionsCommandSpec) {
          // ugly hack because jline cannot dynamically change completer while user types
          if (args[0] instanceof FileArg) {
            for (int i = 0; i < 10; i++) {
              argCompleters.add(new MyFileNameCompleter());
            }
          }
        }
        else {
          for (Arg arg : args) {
            if (arg instanceof FileArg) {
              argCompleters.add(new MyFileNameCompleter());
            }
            else if (arg instanceof CommandArg) {
              argCompleters.add(new StringsCompleter(commands.keySet().toArray(new String[] {})));
            }
            else {
              argCompleters.add(MyNullCompleter.INSTANCE);
            }
          }
        }
      }

      completers.add(new ArgumentCompleter(argCompleters));
    }

    List<Completer> argCompleters = new LinkedList<Completer>();
    Set<String> set = new TreeSet<String>();
    set.addAll(aliases.keySet());
    set.addAll(macros.keySet());
    argCompleters.add(new StringsCompleter(set.toArray(new String[] {})));
    for (int i = 0; i < 10; i++) {
      argCompleters.add(new MyFileNameCompleter());
    }
    completers.add(new ArgumentCompleter(argCompleters));
    return completers;
  }

  private void setupCompleter(ConsoleReader reader)
  {
    reader.addCompleter(new AggregateCompleter(defaultCompleters()));
  }

  private void updateCompleter(ConsoleReader reader)
  {
    List<Completer> completers = new ArrayList<Completer>(reader.getCompleters());
    for (Completer c : completers) {
      reader.removeCompleter(c);
    }
    setupCompleter(reader);
  }

  private void setupHistory(ConsoleReader reader)
  {
    File historyFile = new File(StramClientUtils.getSettingsRootDir(), "cli_history");
    historyFile.getParentFile().mkdirs();
    try {
      topLevelHistory = new FileHistory(historyFile);
      reader.setHistory(topLevelHistory);
      historyFile = new File(StramClientUtils.getSettingsRootDir(), "cli_history_clp");
      changingLogicalPlanHistory = new FileHistory(historyFile);
    }
    catch (IOException ex) {
      System.err.printf("Unable to open %s for writing.", historyFile);
    }
  }

  private void setupAgents() throws IOException
  {
    recordingsAgent = new RecordingsAgent();
    recordingsAgent.setup();
  }

  public void run() throws IOException
  {
    ConsoleReader reader = new ConsoleReader();
    reader.setBellEnabled(false);
    try {
      processSourceFile(System.getProperty("user.home") + "/.dt/clirc_system", reader);
    }
    catch (Exception ex) {
      // ignore
    }
    try {
      processSourceFile(System.getProperty("user.home") + "/.dt/clirc", reader);
    }
    catch (Exception ex) {
      // ignore
    }
    if (consolePresent) {
      printWelcomeMessage();
      //printLicenseStatus();
      setupCompleter(reader);
      setupHistory(reader);
    }
    setupAgents();
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
        line = readLine(reader);
        if (line == null) {
          break;
        }
      }
      processLine(line, reader, true);
      out.flush();
    }
    if (topLevelHistory != null) {
      topLevelHistory.flush();
    }
    if (changingLogicalPlanHistory != null) {
      changingLogicalPlanHistory.flush();
    }
    if (consolePresent) {
      System.out.println("exit");
    }
  }

  private List<String> expandMacro(List<String> lines, String[] args)
  {
    List<String> expandedLines = new ArrayList<String>();

    for (String line : lines) {
      int previousIndex = 0;
      StringBuilder expandedLine = new StringBuilder(line.length());
      while (true) {
        // Search for $0..$9 within the each line and replace by corresponding args
        int currentIndex = line.indexOf('$', previousIndex);
        if (currentIndex > 0 && line.length() > currentIndex + 1) {
          int argIndex = line.charAt(currentIndex + 1) - '0';
          if (args.length > argIndex && argIndex >= 0) {
            // Replace $0 with macro name or $1..$9 with input arguments
            expandedLine.append(line.substring(previousIndex, currentIndex)).append(args[argIndex]);
          }
          else if (argIndex >= 0 && argIndex <= 9) {
            // Arguments for $1..$9 were not supplied - replace with empty strings
            expandedLine.append(line.substring(previousIndex, currentIndex));
          }
          else {
            // Outside valid arguments range - ignore and do not replace
            expandedLine.append(line.substring(previousIndex, currentIndex + 2));
          }
          currentIndex += 2;
        }
        else {
          expandedLine.append(line.substring(previousIndex));
          expandedLines.add(expandedLine.toString());
          break;
        }
        previousIndex = currentIndex;
      }
    }
    return expandedLines;
  }

  private static String ltrim(String s)
  {
    int i = 0;
    while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
      i++;
    }
    return s.substring(i);
  }

  private void processLine(String line, ConsoleReader reader, boolean expandMacroAlias)
  {
    try {
      if (reader.isHistoryEnabled()) {
        History history = reader.getHistory();
        if (history instanceof FileHistory) {
          ((FileHistory)history).flush();
        }
      }
      //LOG.debug("line: \"{}\"", line);
      List<String[]> commands = tokenizer.tokenize(line);
      if (commands == null) {
        return;
      }
      for (String[] args : commands) {
        if (args.length == 0 || StringUtils.isBlank(args[0])) {
          continue;
        }
        //LOG.debug("Got: {}", mapper.writeValueAsString(args));
        if (expandMacroAlias) {
          if (macros.containsKey(args[0])) {
            List<String> macroItems = expandMacro(macros.get(args[0]), args);
            for (String macroItem : macroItems) {
              if (consolePresent) {
                System.out.println("expanded-macro> " + macroItem);
              }
              processLine(macroItem, reader, false);
            }
            continue;
          }

          if (aliases.containsKey(args[0])) {
            processLine(aliases.get(args[0]), reader, false);
            continue;
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
          if (connectedCommands.get(args[0]) != null) {
            System.err.println("\"" + args[0] + "\" is valid only when connected to an application. Type \"connect <appid>\" to connect to an application.");
          }
          else if (logicalPlanChangeCommands.get(args[0]) != null) {
            System.err.println("\"" + args[0] + "\" is valid only when changing a logical plan.  Type \"begin-logical-plan-change\" to change a logical plan");
          }
          else {
            System.err.println("Invalid command '" + args[0] + "'. Type \"help\" for list of commands");
          }
        }
        else {
          try {
            cs.verifyArguments(args);
          }
          catch (CliException ex) {
            cs.printUsage(args[0]);
            throw ex;
          }
          cs.command.execute(args, reader);
          lastCommandError = false;
        }
      }
    }
    catch (CliException e) {
      System.err.println(e.getMessage());
      LOG.debug("Error processing line: " + line, e);
      lastCommandError = true;
    }
    catch (Exception e) {
      System.err.println("Unexpected error: " + e);
      LOG.error("Error processing line: {}", line, e);
      lastCommandError = true;
    }
  }

  private void printWelcomeMessage()
  {
    System.out.println("DT CLI " + VersionInfo.getVersion() + " " + VersionInfo.getDate() + " " + VersionInfo.getRevision());
  }

  private void printLicenseStatus()
  {
    try {
      JSONObject licenseStatus = getLicenseStatus(null);
      if (!licenseStatus.has("agentAppId")) {
        System.out.println("License agent is not running. Please run the license agent first by typing \"activate-license\"");
        return;
      }
      if (licenseStatus.has("remainingLicensedMB")) {
        int remainingLicensedMB = licenseStatus.getInt("remainingLicensedMB");
        if (remainingLicensedMB > 0) {
          System.out.println("You have " + remainingLicensedMB + "MB remaining for the current license.");
        }
        else {
          System.out.println("You do not have any memory allowance left for the current license. Please contact DataTorrent, Inc. <support@datatorrent.com> for help.");
        }
      }
    }
    catch (Exception ex) {
      LOG.error("Caught exception when getting license info", ex);
      System.out.println("Error getting license status. Please contact DataTorrent, Inc. <support@datatorrent.com> for help.");
    }
  }

  private void printHelp(String command, CommandSpec commandSpec, PrintStream os)
  {
    if (consolePresent) {
      os.print("\033[0;93m");
      os.print(command);
      os.print("\033[0m");
    }
    else {
      os.print(command);
    }
    if (commandSpec instanceof OptionsCommandSpec) {
      OptionsCommandSpec ocs = (OptionsCommandSpec)commandSpec;
      if (ocs.options != null) {
        os.print(" [options]");
      }
    }
    if (commandSpec.requiredArgs != null) {
      for (Arg arg : commandSpec.requiredArgs) {
        if (consolePresent) {
          os.print(" \033[3m" + arg + "\033[0m");
        }
        else {
          os.print(" <" + arg + ">");
        }
      }
    }
    if (commandSpec.optionalArgs != null) {
      for (Arg arg : commandSpec.optionalArgs) {
        if (consolePresent) {
          os.print(" [\033[3m" + arg + "\033[0m]");
        }
        else {
          os.print(" [<" + arg + ">]");
        }
      }
    }
    os.println("\n\t" + commandSpec.description);
    if (commandSpec instanceof OptionsCommandSpec) {
      OptionsCommandSpec ocs = (OptionsCommandSpec)commandSpec;
      if (ocs.options != null) {
        os.println("\tOptions:");
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter pw = new PrintWriter(os);
        formatter.printOptions(pw, 80, ocs.options, 12, 4);
        pw.flush();
      }
    }
  }

  private void printHelp(Map<String, CommandSpec> commandSpecs, PrintStream os)
  {
    for (Map.Entry<String, CommandSpec> entry : commandSpecs.entrySet()) {
      printHelp(entry.getKey(), entry.getValue(), os);
    }
  }

  private String readLine(ConsoleReader reader)
          throws IOException
  {
    String prompt = "";
    if (consolePresent) {
      if (changingLogicalPlan) {
        prompt = "logical-plan-change";
      }
      else {
        prompt = "dt";
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
    return ltrim(line);
  }

  private List<ApplicationReport> getApplicationList()
  {
    try {
      GetApplicationsRequest appsReq = GetApplicationsRequest.newInstance();
      appsReq.setApplicationTypes(Sets.newHashSet(StramClient.YARN_APPLICATION_TYPE));
      return rmClient.clientRM.getApplications(appsReq).getApplicationList();
    }
    catch (Exception e) {
      throw new CliException("Error getting application list from resource manager: " + e.getMessage(), e);
    }
  }

  private List<ApplicationReport> getRunningApplicationList()
  {
    try {
      GetApplicationsRequest appsReq = GetApplicationsRequest.newInstance();
      appsReq.setApplicationTypes(Sets.newHashSet(StramClient.YARN_APPLICATION_TYPE));
      appsReq.setApplicationStates(EnumSet.of(YarnApplicationState.RUNNING));
      return rmClient.clientRM.getApplications(appsReq).getApplicationList();
    }
    catch (Exception e) {
      throw new CliException("Error getting application list from resource manager: " + e.getMessage(), e);
    }
  }

  private List<ApplicationReport> getLicenseAgentList()
  {
    try {
      GetApplicationsRequest appsReq = GetApplicationsRequest.newInstance();
      appsReq.setApplicationTypes(Sets.newHashSet(StramClient.YARN_APPLICATION_TYPE_LICENSE));
      appsReq.setApplicationStates(EnumSet.of(YarnApplicationState.RUNNING));
      return rmClient.clientRM.getApplications(appsReq).getApplicationList();
    }
    catch (Exception e) {
      throw new CliException("Error getting application list from resource manager: " + e.getMessage(), e);
    }
  }

  private String getContainerLongId(String containerId)
  {
    ClientResponse rsp = getResource(StramWebServices.PATH_PHYSICAL_PLAN_CONTAINERS, currentApp);
    JSONObject json = rsp.getEntity(JSONObject.class);
    int shortId = 0;
    if (StringUtils.isNumeric(containerId)) {
      shortId = Integer.parseInt(containerId);
    }
    try {
      Object containersObj = json.get("containers");
      JSONArray containers;
      if (containersObj instanceof JSONArray) {
        containers = (JSONArray)containersObj;
      }
      else {
        containers = new JSONArray();
        containers.put(containersObj);
      }
      if (containersObj != null) {
        for (int o = containers.length(); o-- > 0;) {
          JSONObject container = containers.getJSONObject(o);
          String id = container.getString("id");
          if (id.equals(containerId) || (shortId != 0 && (id.endsWith("_" + shortId) || id.endsWith("0" + shortId)))) {
            return id;
          }
        }
      }
    }
    catch (JSONException ex) {
    }
    return null;
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
    catch (YarnException rmExc) {
      throw new CliException("Unable to determine application status.", rmExc);
    }
    catch (IOException rmExc) {
      throw new CliException("Unable to determine application status.", rmExc);
    }
    return r;
  }

  private ClientResponse getResource(String resourcePath, ApplicationReport appReport)
  {

    if (appReport == null) {
      throw new CliException("No application selected");
    }

    if (StringUtils.isEmpty(appReport.getTrackingUrl()) || appReport.getFinalApplicationStatus() != FinalApplicationStatus.UNDEFINED) {
      appReport = null;
      throw new CliException("Application terminated.");
    }

    WebServicesClient wsClient = new WebServicesClient();
    Client client = wsClient.getClient();
    client.setFollowRedirects(true);
    WebResource r;

    try {
      r = StramAgent.getStramWebResource(wsClient, appReport.getApplicationId().toString());
    }
    catch (IncompatibleVersionException ex) {
      throw new CliException("Incompatible stram version", ex);
    }
    if (r == null) {
      throw new CliException("Application " + appReport.getApplicationId().toString() + " has not started");
    }
    r = r.path(resourcePath);

    try {
      return wsClient.process(r, ClientResponse.class, new WebServicesClient.WebServicesHandler<ClientResponse>()
      {
        @Override
        public ClientResponse process(WebResource webResource, Class<ClientResponse> clazz)
        {
          ClientResponse response = webResource.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
          if (!MediaType.APPLICATION_JSON_TYPE.equals(response.getType())) {
            throw new CliException("Unexpected response type " + response.getType());
          }
          return response;
        }

      });
    }
    catch (Exception e) {
      // check the application status as above may have failed due application termination etc.
      if (appReport == currentApp) {
        currentApp = assertRunningApp(appReport);
      }
      throw new CliException("Failed to request " + r.getURI(), e);
    }
  }

  private WebResource getStramWebResource(WebServicesClient webServicesClient, ApplicationReport appReport)
  {
    if (appReport == null) {
      throw new CliException("No application selected");
    }
    // YARN-156 WebAppProxyServlet does not support POST - for now bypass it for this request
    appReport = assertRunningApp(appReport); // or else "N/A" might be there..
    try {
      return StramAgent.getStramWebResource(webServicesClient, appReport.getApplicationId().toString());
    }
    catch (IncompatibleVersionException ex) {
      throw new CliException("Incompatible Stram version", ex);
    }
  }

  private List<AppFactory> getMatchingAppFactories(StramAppLauncher submitApp, String matchString)
  {
    try {
      List<AppFactory> cfgList = submitApp.getBundledTopologies();

      if (cfgList.isEmpty()) {
        return null;
      }
      else if (matchString == null) {
        return cfgList;
      }
      else {
        List<AppFactory> result = new ArrayList<AppFactory>();
        for (AppFactory ac : cfgList) {
          if (ac.getName().toLowerCase().contains(matchString.toLowerCase())) {
            result.add(ac);
          }
        }
        return result;
      }
    }
    catch (Exception ex) {
      return null;
    }
  }

  /*
   * Below is the implementation of all commands
   */
  private class HelpCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      PrintStream os = getOutputPrintStream();
      if (args.length < 2) {
        os.println("GLOBAL COMMANDS EXCEPT WHEN CHANGING LOGICAL PLAN:\n");
        printHelp(globalCommands, os);
        os.println();
        os.println("COMMANDS WHEN CONNECTED TO AN APP (via connect <appid>) EXCEPT WHEN CHANGING LOGICAL PLAN:\n");
        printHelp(connectedCommands, os);
        os.println();
        os.println("COMMANDS WHEN CHANGING LOGICAL PLAN (via begin-logical-plan-change):\n");
        printHelp(logicalPlanChangeCommands, os);
        os.println();
      }
      else {
        if (args[1].equals("help")) {
          printHelp("help", globalCommands.get("help"), os);
        }
        else {
          boolean valid = false;
          CommandSpec cs = globalCommands.get(args[1]);
          if (cs != null) {
            os.println("This usage is valid except when changing logical plan");
            printHelp(args[1], cs, os);
            os.println();
            valid = true;
          }
          cs = connectedCommands.get(args[1]);
          if (cs != null) {
            os.println("This usage is valid when connected to an app except when changing logical plan");
            printHelp(args[1], cs, os);
            os.println();
            valid = true;
          }
          cs = logicalPlanChangeCommands.get(args[1]);
          if (cs != null) {
            os.println("This usage is only valid when changing logical plan (via begin-logical-plan-change)");
            printHelp(args[1], cs, os);
            os.println();
            valid = true;
          }
          if (!valid) {
            os.println("Help for \"" + args[1] + "\" does not exist.");
          }
        }
      }
      closeOutputPrintStream(os);
    }

  }

  private class ConnectCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {

      currentApp = getApplication(args[1]);
      if (currentApp == null) {
        throw new CliException("Invalid application id: " + args[1]);
      }

      boolean connected = false;
      try {
        LOG.debug("Selected {} with tracking url {}", currentApp.getApplicationId(), currentApp.getTrackingUrl());
        ClientResponse rsp = getResource(StramWebServices.PATH_INFO, currentApp);
        rsp.getEntity(JSONObject.class);
        connected = true; // set as current only upon successful connection
        if (consolePresent) {
          System.out.println("Connected to application " + currentApp.getApplicationId());
        }
      }
      catch (CliException e) {
        throw e; // pass on
      }
      finally {
        if (!connected) {
          //currentApp = null;
          //currentDir = "/";
        }
      }
    }

  }

  private class ActivateLicenseCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String file = null;
      if (args.length > 1) {
        file = args[1];
      }
      String licenseId = activateLicense(file);
      System.out.println("Started license agent for " + licenseId);
    }

  }

  protected String activateLicense(String file) throws Exception
  {
    byte[] licenseBytes;
    if (file != null) {
      licenseBytes = StramClientUtils.getLicense(file);
    }
    else {
      licenseBytes = StramClientUtils.getLicense(conf);
    }
    String licenseId = License.getLicenseID(licenseBytes);
    License.validateLicense(licenseBytes);
    LogicalPlan lp = new LogicalPlan();
    lp.setAttribute(DAG.APPLICATION_NAME, licenseId);
    lp.setAttribute(LogicalPlan.LICENSE, Base64.encodeBase64String(licenseBytes)); // TODO: obfuscate license passing
    int licenseMasterMemoryMB = StramClientUtils.getLicenseMasterMemory(conf);
    lp.setAttribute(DAGContext.MASTER_MEMORY_MB, licenseMasterMemoryMB);
    StramClient client = new StramClient(lp);
    client.setApplicationType(StramClient.YARN_APPLICATION_TYPE_LICENSE);
    client.startApplication();
    return licenseId;
  }

  private class DeactivateLicenseCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String licenseId;
      if (args.length > 1) {
        licenseId = args[1];
      }
      else {
        byte[] licenseBytes;
        licenseBytes = StramClientUtils.getLicense(conf);
        licenseId = License.getLicenseID(licenseBytes);
        License.validateLicense(licenseBytes);
      }
      // TODO: migrate CLI to use YarnClient and this here won't be needed
      YarnClient clientRMService = YarnClient.createYarnClient();
      try {
        clientRMService.init(conf);
        clientRMService.start();
        ApplicationReport ar = LicensingAgentProtocolHelper.getLicensingAgentAppReport(licenseId, clientRMService);
        if (ar == null) {
          throw new CliException("License not activated: " + licenseId);
        }
        rmClient.killApplication(ar.getApplicationId());
        System.out.println("Stopped license agent for " + licenseId);
      }
      finally {
        clientRMService.stop();
      }
    }

  }

  private static class LicenseInfo
  {
    int remainingLicensedMB;
    int totalLicensedMB;
    long lastUpdate;
    // add expiration date range here
  }

  private Map<String, LicenseInfo> getLicenseInfoMap()
  {
    List<ApplicationReport> runningApplicationList = getRunningApplicationList();
    WebServicesClient webServicesClient = new WebServicesClient();

    Map<String, LicenseInfo> licenseInfoMap = new HashMap<String, LicenseInfo>();

    for (ApplicationReport ar : runningApplicationList) {
      try {
        WebResource r = getStramWebResource(webServicesClient, ar);
        if (r == null) {
          throw new Exception("Cannot get stram web resource for " + ar.getApplicationId());
        }
        r = r.path(StramWebServices.PATH_INFO);

        JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
          {
            return webResource.accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
          }

        });
        if (!response.has("licenseInfoLastUpdate")) {
          continue;
        }
        long lastUpdate = Long.valueOf(response.getString("licenseInfoLastUpdate"));
        String licenseId = response.getString("licenseId");
        int remainingLicensedMB = Integer.valueOf(response.getString("remainingLicensedMB"));
        int totalLicensedMB = Integer.valueOf(response.getString("totalLicensedMB"));

        LicenseInfo licenseInfo;

        if (licenseInfoMap.containsKey(licenseId)) {
          licenseInfo = licenseInfoMap.get(licenseId);
          if (licenseInfo.lastUpdate < lastUpdate) {
            licenseInfo.remainingLicensedMB = remainingLicensedMB;
            licenseInfo.totalLicensedMB = totalLicensedMB;
            licenseInfo.lastUpdate = lastUpdate;
          }
        }
        else {
          licenseInfo = new LicenseInfo();
          licenseInfo.remainingLicensedMB = remainingLicensedMB;
          licenseInfo.totalLicensedMB = totalLicensedMB;
          licenseInfo.lastUpdate = lastUpdate;
          licenseInfoMap.put(licenseId, licenseInfo);
        }
      }
      catch (Exception ex) {
        LOG.warn("Caught exception when trying to get information about application {}", ar.getApplicationId(), ex);
        continue;
      }
    }
    return licenseInfoMap;
  }

  private class ListLicenseAgentsCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      Map<String, LicenseInfo> licenseInfoMap = getLicenseInfoMap();

      try {
        JSONArray jsonArray = new JSONArray();
        List<ApplicationReport> licList = getLicenseAgentList();
        Collections.sort(licList, new Comparator<ApplicationReport>()
        {
          @Override
          public int compare(ApplicationReport o1, ApplicationReport o2)
          {
            return o1.getApplicationId().getId() - o2.getApplicationId().getId();
          }

        });

        for (ApplicationReport ar : licList) {
          JSONObject jsonObj = new JSONObject();
          jsonObj.put("id", ar.getName());
          jsonObj.put("agentAppId", ar.getApplicationId().getId());
          if (licenseInfoMap.containsKey(ar.getName())) {
            jsonObj.put("remainingLicensedMB", licenseInfoMap.get(ar.getName()).remainingLicensedMB);
            jsonObj.put("totalLicensedMB", licenseInfoMap.get(ar.getName()).totalLicensedMB);
          }
          jsonArray.put(jsonObj);
        }
        printJson(jsonArray, "licenses");
      }
      catch (Exception ex) {
        throw new CliException("Failed to retrieve license list", ex);
      }
    }

  }

  private JSONObject getLicenseStatus(String licenseFile) throws Exception
  {
    byte[] licenseBytes;
    if (licenseFile != null) {
      licenseBytes = StramClientUtils.getLicense(licenseFile);
    }
    else {
      licenseBytes = StramClientUtils.getLicense(conf);
    }
    String licenseID = License.getLicenseID(licenseBytes);
    LicenseSection[] licenseSections = License.validateGetLicenseSections(licenseBytes);
    JSONObject licenseObj = new JSONObject();
    licenseObj.put("id", licenseID);

    JSONArray sectionArr = new JSONArray();

    SimpleDateFormat sdf = new SimpleDateFormat(LicenseSection.DATE_FORMAT);
    for (LicenseSection section : licenseSections) {
      JSONObject sectionObj = new JSONObject();
      sectionObj.put("startDate", sdf.format(section.getStartDate()));
      sectionObj.put("endDate", sdf.format(section.getEndDate()));
      sectionObj.put("comment", section.getComment());
      sectionObj.put("processorList", section.getLicenseSectionInfoAsJSONObj());
      sectionObj.put("constraint", section.getConstraint());
      sectionObj.put("url", section.getUrl());
      sectionArr.put(sectionObj);
    }
    licenseObj.put("sections", sectionArr);
    List<ApplicationReport> licList = getLicenseAgentList();
    for (ApplicationReport ar : licList) {
      if (ar.getName().equals(licenseID)) {
        licenseObj.put("agentAppId", ar.getApplicationId().toString());
        break;
      }
    }
    Map<String, LicenseInfo> licenseInfoMap = getLicenseInfoMap();
    if (licenseInfoMap.containsKey(licenseID)) {
      licenseObj.put("remainingLicensedMB", licenseInfoMap.get(licenseID).remainingLicensedMB);
      licenseObj.put("totalLicensedMB", licenseInfoMap.get(licenseID).totalLicensedMB);
    }
    return licenseObj;
  }

  private class ShowLicenseStatusCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      JSONObject licenseObj = getLicenseStatus(args.length > 1 ? args[1] : null);
      printJson(licenseObj);
    }

  }

  private class GenerateLicenseRequestCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String b64EncodedString = new GenerateLicenseRequest().getLicenseRequest(Util.getDefaultPublicKey());
      System.out.println("-------------------------- Cut from below ------------------------------");
      System.out.println(b64EncodedString);
      System.out.println("------------------------------------------------------------------------");
    }

  }

  private class LaunchCommand implements Command
  {
    @Override
    @SuppressWarnings("SleepWhileInLoop")
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String[] newArgs = new String[args.length - 1];
      System.arraycopy(args, 1, newArgs, 0, args.length - 1);
      LaunchCommandLineInfo commandLineInfo = getLaunchCommandLineInfo(newArgs);

      if (commandLineInfo.configFile != null) {
        commandLineInfo.configFile = expandFileName(commandLineInfo.configFile, true);
      }
      Configuration config = StramAppLauncher.getConfig(commandLineInfo.configFile, commandLineInfo.overrideProperties);
      if (commandLineInfo.libjars != null) {
        commandLineInfo.libjars = expandCommaSeparatedFiles(commandLineInfo.libjars);
        config.set(StramAppLauncher.LIBJARS_CONF_KEY_NAME, commandLineInfo.libjars);
      }
      if (commandLineInfo.files != null) {
        commandLineInfo.files = expandCommaSeparatedFiles(commandLineInfo.files);
        config.set(StramAppLauncher.FILES_CONF_KEY_NAME, commandLineInfo.files);
      }
      if (commandLineInfo.archives != null) {
        commandLineInfo.archives = expandCommaSeparatedFiles(commandLineInfo.archives);
        config.set(StramAppLauncher.ARCHIVES_CONF_KEY_NAME, commandLineInfo.archives);
      }
      if (commandLineInfo.licenseFile != null) {
        commandLineInfo.licenseFile = expandFileName(commandLineInfo.licenseFile, true);
      }
      String fileName = expandFileName(commandLineInfo.args[0], true);
      StramAppLauncher submitApp = getStramAppLauncher(fileName, config, commandLineInfo.ignorePom);
      submitApp.loadDependencies();
      AppFactory appFactory = null;
      if (commandLineInfo.args.length >= 2) {
        File file = new File(commandLineInfo.args[1]);
        if (file.exists()) {
          appFactory = new StramAppLauncher.PropertyFileAppFactory(file);
        }
      }

      if (appFactory == null) {
        String matchString = commandLineInfo.args.length >= 2 ? commandLineInfo.args[1] : null;

        List<AppFactory> matchingAppFactories = getMatchingAppFactories(submitApp, matchString);
        if (matchingAppFactories == null || matchingAppFactories.isEmpty()) {
          throw new CliException("No matching applications bundled in jar.");
        }
        else if (matchingAppFactories.size() == 1) {
          appFactory = matchingAppFactories.get(0);
        }
        else if (matchingAppFactories.size() > 1) {

          // Display matching applications
          for (int i = 0; i < matchingAppFactories.size(); i++) {
            String appName = matchingAppFactories.get(i).getName();
            String appAlias = submitApp.getLogicalPlanConfiguration().getAppAlias(appName);
            if (appAlias != null) {
              appName = appAlias;
            }
            System.out.printf("%3d. %s\n", i + 1, appName);
          }

          // Exit if not in interactive mode
          if (!consolePresent) {
            throw new CliException("More than one application in jar file match '" + matchString + "'");
          }
          else {

            boolean useHistory = reader.isHistoryEnabled();
            reader.setHistoryEnabled(false);
            History previousHistory = reader.getHistory();
            History dummyHistory = new MemoryHistory();
            reader.setHistory(dummyHistory);
            List<Completer> completers = new ArrayList<Completer>(reader.getCompleters());
            for (Completer c : completers) {
              reader.removeCompleter(c);
            }
            String optionLine = reader.readLine("Choose application: ");
            reader.setHistoryEnabled(useHistory);
            reader.setHistory(previousHistory);
            for (Completer c : completers) {
              reader.addCompleter(c);
            }

            try {
              int option = Integer.parseInt(optionLine);
              if (0 < option && option <= matchingAppFactories.size()) {
                appFactory = matchingAppFactories.get(option - 1);
              }
            }
            catch (Exception ex) {
              // ignore
            }
          }
        }

      }

      if (appFactory != null) {
        if (!commandLineInfo.localMode) {

          byte[] licenseBytes;
          if (commandLineInfo.licenseFile != null) {
            licenseBytes = StramClientUtils.getLicense(commandLineInfo.licenseFile);
          }
          else {
            licenseBytes = StramClientUtils.getLicense(conf);
          }
          // This is for suppressing System.out printouts from applications so that the user of CLI will not be confused by those printouts
          PrintStream originalStream = System.out;
          ApplicationId appId = null;
          YarnClient clientRMService = YarnClient.createYarnClient();
          try {
            if (raw) {
              PrintStream dummyStream = new PrintStream(new OutputStream()
              {
                @Override
                public void write(int b)
                {
                  // no-op
                }

              });
              System.setOut(dummyStream);
            }
            String licenseId = License.getLicenseID(licenseBytes);
            clientRMService.init(conf);
            clientRMService.start();
            ApplicationReport ar = LicensingAgentProtocolHelper.getLicensingAgentAppReport(licenseId, clientRMService);
            if (ar == null) {
              try {
                LOG.debug("License agent is not running for {}. Trying to automatically start a license agent.", licenseId);
                activateLicense(null);
                long timeout = System.currentTimeMillis() + TIMEOUT_AFTER_ACTIVATE_LICENSE;
                do {
                  Thread.sleep(1000);
                  ar = LicensingAgentProtocolHelper.getLicensingAgentAppReport(licenseId, clientRMService);
                  if (ar == null && !raw) {
                    System.out.println("Waiting for license agent to start...");
                  }
                }
                while (ar == null && System.currentTimeMillis() <= timeout);
              }
              catch (Exception ex) {
                throw new CliException("Trouble activating license. Please contact <support@datatorrent.com> for help.", ex);
              }
              if (ar == null) {
                throw new CliException("Trouble activating license. Please contact <support@datatorrent.com> for help.");
              }
            }
            appId = submitApp.launchApp(appFactory);
            currentApp = rmClient.getApplicationReport(appId);
          }
          finally {
            clientRMService.stop();
            if (raw) {
              System.setOut(originalStream);
            }
          }
          if (appId != null) {
            printJson("{\"appId\": \"" + appId + "\"}");
          }
        }
        else {
          submitApp.runLocal(appFactory);
        }
      }
      else {
        System.err.println("No application specified.");
      }

    }

  }

  private class ShutdownAppCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      ApplicationReport[] apps;
      WebServicesClient webServicesClient = new WebServicesClient();
      if (args.length == 1) {
        if (currentApp == null) {
          throw new CliException("No application selected");
        }
        else {
          apps = new ApplicationReport[] {currentApp};
        }
      }
      else {
        apps = new ApplicationReport[args.length - 1];
        for (int i = 1; i < args.length; i++) {
          apps[i - 1] = getApplication(args[i]);
          if (apps[i - 1] == null) {
            throw new CliException("App " + args[i] + " not found!");
          }
        }
      }

      for (ApplicationReport app : apps) {
        WebResource r = getStramWebResource(webServicesClient, app).path(StramWebServices.PATH_SHUTDOWN);
        try {
          JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
          {
            @Override
            public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
            {
              return webResource.accept(MediaType.APPLICATION_JSON).post(clazz);
            }

          });
          if (consolePresent) {
            System.out.println("Shutdown requested: " + response);
          }
          currentApp = null;
        }
        catch (Exception e) {
          throw new CliException("Failed to request " + r.getURI(), e);
        }
      }
    }

  }

  private class ListAppsCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      try {
        JSONArray jsonArray = new JSONArray();
        List<ApplicationReport> appList = getApplicationList();
        Collections.sort(appList, new Comparator<ApplicationReport>()
        {
          @Override
          public int compare(ApplicationReport o1, ApplicationReport o2)
          {
            return o1.getApplicationId().getId() - o2.getApplicationId().getId();
          }

        });
        int totalCnt = 0;
        int runningCnt = 0;

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");

        for (ApplicationReport ar : appList) {
          /*
           * This is inefficient, but what the heck, if this can be passed through the command line, can anyone notice slowness.
           */
          JSONObject jsonObj = new JSONObject();
          jsonObj.put("startTime", sdf.format(new java.util.Date(ar.getStartTime())));
          jsonObj.put("id", ar.getApplicationId().getId());
          jsonObj.put("name", ar.getName());
          jsonObj.put("state", ar.getYarnApplicationState().name());
          jsonObj.put("trackingUrl", ar.getTrackingUrl());
          jsonObj.put("finalStatus", ar.getFinalApplicationStatus());

          totalCnt++;
          if (ar.getYarnApplicationState() == YarnApplicationState.RUNNING) {
            runningCnt++;
          }

          if (args.length > 1) {
            if (StringUtils.isNumeric(args[1])) {
              if (jsonObj.getString("id").equals(args[1])) {
                jsonArray.put(jsonObj);
                break;
              }
            }
            else {
              @SuppressWarnings("unchecked")
              Iterator<String> keys = jsonObj.keys();
              while (keys.hasNext()) {
                if (jsonObj.get(keys.next()).toString().toLowerCase().contains(args[1].toLowerCase())) {
                  jsonArray.put(jsonObj);
                  break;
                }
              }
            }
          }
          else {
            jsonArray.put(jsonObj);
          }
        }
        printJson(jsonArray, "apps");
        if (consolePresent) {
          System.out.println(runningCnt + " active, total " + totalCnt + " applications.");
        }
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
          catch (YarnException e) {
            throw new CliException("Failed to kill " + currentApp.getApplicationId(), e);
          }
        }
        if (consolePresent) {
          System.out.println("Kill app requested");
        }
        return;
      }

      ApplicationReport app = null;
      int i = 0;
      try {
        while (++i < args.length) {
          app = getApplication(args[i]);
          if (app == null) {
            throw new CliException("Streaming application with id " + args[i] + " is not found.");
          }
          rmClient.killApplication(app.getApplicationId());
          if (app == currentApp) {
            currentApp = null;
          }
        }
        if (consolePresent) {
          System.out.println("Kill app requested");
        }
      }
      catch (YarnException e) {
        throw new CliException("Failed to kill " + ((app == null || app.getApplicationId() == null) ? "unknown application" : app.getApplicationId()) + ". Aborting killing of any additional applications.", e);
      }
      catch (NumberFormatException nfe) {
        throw new CliException("Invalid application Id " + args[i], nfe);
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
      if (consolePresent) {
        System.out.println("Alias " + args[1] + " created.");
      }
      updateCompleter(reader);
    }

  }

  private class SourceCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      processSourceFile(args[1], reader);
      if (consolePresent) {
        System.out.println("File " + args[1] + " sourced.");
      }
    }

  }

  private class ExitCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      if (topLevelHistory != null) {
        topLevelHistory.flush();
      }
      if (changingLogicalPlanHistory != null) {
        changingLogicalPlanHistory.flush();
      }
      System.exit(0);
    }

  }

  private class ListContainersCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      ClientResponse rsp = getResource(StramWebServices.PATH_PHYSICAL_PLAN_CONTAINERS, currentApp);
      JSONObject json = rsp.getEntity(JSONObject.class);
      if (args.length == 1) {
        printJson(json);
      }
      else {
        Object containersObj = json.get("containers");
        JSONArray containers;
        if (containersObj instanceof JSONArray) {
          containers = (JSONArray)containersObj;
        }
        else {
          containers = new JSONArray();
          containers.put(containersObj);
        }
        if (containersObj == null) {
          System.out.println("No containers found!");
        }
        else {
          JSONArray resultContainers = new JSONArray();
          for (int o = containers.length(); o-- > 0;) {
            JSONObject container = containers.getJSONObject(o);
            String id = container.getString("id");
            if (id != null && !id.isEmpty()) {
              for (int argc = args.length; argc-- > 1;) {
                String s1 = "0" + args[argc];
                String s2 = "_" + args[argc];
                if (id.equals(args[argc]) || id.endsWith(s1) || id.endsWith(s2)) {
                  resultContainers.put(container);
                }
              }
            }
          }
          printJson(resultContainers, "containers");
        }
      }
    }

  }

  private class ListOperatorsCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      ClientResponse rsp = getResource(StramWebServices.PATH_PHYSICAL_PLAN_OPERATORS, currentApp);
      JSONObject json = rsp.getEntity(JSONObject.class);

      if (args.length > 1) {
        String singleKey = "" + json.keys().next();
        JSONArray matches = new JSONArray();
        // filter operators
        JSONArray arr;
        Object obj = json.get(singleKey);
        if (obj instanceof JSONArray) {
          arr = (JSONArray)obj;
        }
        else {
          arr = new JSONArray();
          arr.put(obj);
        }
        for (int i = 0; i < arr.length(); i++) {
          JSONObject oper = arr.getJSONObject(i);
          if (StringUtils.isNumeric(args[1])) {
            if (oper.getString("id").equals(args[1])) {
              matches.put(oper);
              break;
            }
          }
          else {
            @SuppressWarnings("unchecked")
            Iterator<String> keys = oper.keys();
            while (keys.hasNext()) {
              if (oper.get(keys.next()).toString().toLowerCase().contains(args[1].toLowerCase())) {
                matches.put(oper);
                break;
              }
            }
          }
        }
        json.put(singleKey, matches);
      }

      printJson(json);
    }

  }

  private class ShowPhysicalPlanCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      WebServicesClient webServicesClient = new WebServicesClient();
      WebResource r = getStramWebResource(webServicesClient, currentApp).path(StramWebServices.PATH_PHYSICAL_PLAN);
      try {
        printJson(webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
          {
            return webResource.accept(MediaType.APPLICATION_JSON).get(clazz);
          }

        }));
      }
      catch (Exception e) {
        throw new CliException("Failed to request " + r.getURI(), e);
      }
    }

  }

  private class KillContainerCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String containerLongId = getContainerLongId(args[1]);
      if (containerLongId == null) {
        throw new CliException("Container " + args[1] + " not found");
      }
      WebServicesClient webServicesClient = new WebServicesClient();
      WebResource r = getStramWebResource(webServicesClient, currentApp).path(StramWebServices.PATH_PHYSICAL_PLAN_CONTAINERS).path(containerLongId).path("kill");
      try {
        JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
          {
            return webResource.accept(MediaType.APPLICATION_JSON).post(clazz, new JSONObject());
          }

        });
        if (consolePresent) {
          System.out.println("Kill container requested: " + response);
        }
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
      catch (YarnException e) {
        throw new CliException("Failed to kill " + currentApp.getApplicationId(), e);
      }
    }

  }

  private class StartRecordingCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String opId = args[1];
      String port = null;
      if (args.length == 3) {
        port = args[2];
      }
      printJson(recordingsAgent.startRecording(currentApp.getApplicationId().toString(), opId, port));
    }

  }

  private class StopRecordingCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String opId = args[1];
      String port = null;
      if (args.length == 3) {
        port = args[2];
      }
      printJson(recordingsAgent.stopRecording(currentApp.getApplicationId().toString(), opId, port));
    }

  }

  private class GetRecordingInfoCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      if (args.length <= 1) {
        List<RecordingInfo> recordingInfo = recordingsAgent.getRecordingInfo(currentApp.getApplicationId().toString());
        printJson(recordingInfo, "recordings");
      }
      else if (args.length <= 2) {
        String opId = args[1];
        List<RecordingInfo> recordingInfo = recordingsAgent.getRecordingInfo(currentApp.getApplicationId().toString(), opId);
        printJson(recordingInfo, "recordings");
      }
      else {
        String opId = args[1];
        long startTime = Long.valueOf(args[2]);
        RecordingInfo recordingInfo = recordingsAgent.getRecordingInfo(currentApp.getApplicationId().toString(), opId, startTime);
        printJson(new JSONObject(mapper.writeValueAsString(recordingInfo)));
      }
    }

  }

  private class GetAppAttributesCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      if (currentApp == null) {
        throw new CliException("No application selected");
      }
      WebServicesClient webServicesClient = new WebServicesClient();
      WebResource r = getStramWebResource(webServicesClient, currentApp).path(StramWebServices.PATH_LOGICAL_PLAN).path("attributes");
      if (args.length > 1) {
        r = r.queryParam("attributeName", args[1]);
      }
      try {
        JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
          {
            return webResource.accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
          }

        });
        printJson(response);
      }
      catch (Exception e) {
        throw new CliException("Failed to request " + r.getURI(), e);
      }
    }

  }

  private class GetOperatorAttributesCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      if (currentApp == null) {
        throw new CliException("No application selected");
      }
      WebServicesClient webServicesClient = new WebServicesClient();
      WebResource r = getStramWebResource(webServicesClient, currentApp).path(StramWebServices.PATH_LOGICAL_PLAN_OPERATORS).path(args[1]).path("attributes");
      if (args.length > 2) {
        r = r.queryParam("attributeName", args[2]);
      }
      try {
        JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
          {
            return webResource.accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
          }

        });
        printJson(response);
      }
      catch (Exception e) {
        throw new CliException("Failed to request " + r.getURI(), e);
      }
    }

  }

  private class GetPortAttributesCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      if (currentApp == null) {
        throw new CliException("No application selected");
      }
      WebServicesClient webServicesClient = new WebServicesClient();
      WebResource r = getStramWebResource(webServicesClient, currentApp).path(StramWebServices.PATH_LOGICAL_PLAN_OPERATORS).path(args[1]).path(args[2]).path("attributes");
      if (args.length > 3) {
        r = r.queryParam("attributeName", args[3]);
      }
      try {
        JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
          {
            return webResource.accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
          }

        });
        printJson(response);
      }
      catch (Exception e) {
        throw new CliException("Failed to request " + r.getURI(), e);
      }
    }

  }

  private class GetOperatorPropertiesCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      if (currentApp == null) {
        throw new CliException("No application selected");
      }
      WebServicesClient webServicesClient = new WebServicesClient();
      WebResource r = getStramWebResource(webServicesClient, currentApp).path(StramWebServices.PATH_LOGICAL_PLAN_OPERATORS).path(args[1]).path("properties");
      if (args.length > 2) {
        r = r.queryParam("propertyName", args[2]);
      }
      try {
        JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
          {
            return webResource.accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
          }

        });
        printJson(response);
      }
      catch (Exception e) {
        throw new CliException("Failed to request " + r.getURI(), e);
      }
    }

  }

  private class GetPhysicalOperatorPropertiesCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      if (currentApp == null) {
        throw new CliException("No application selected");
      }
      WebServicesClient webServicesClient = new WebServicesClient();
      WebResource r = getStramWebResource(webServicesClient, currentApp).path(StramWebServices.PATH_PHYSICAL_PLAN_OPERATORS).path(args[1]).path("properties");
      if (args.length > 2) {
        r = r.queryParam("propertyName", args[2]);
      }
      try {
        JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
          {
            return webResource.accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
          }

        });
        printJson(response);
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
      if (currentApp == null) {
        throw new CliException("No application selected");
      }
      if (changingLogicalPlan) {
        String operatorName = args[1];
        String propertyName = args[2];
        String propertyValue = args[3];
        SetOperatorPropertyRequest request = new SetOperatorPropertyRequest();
        request.setOperatorName(operatorName);
        request.setPropertyName(propertyName);
        request.setPropertyValue(propertyValue);
        logicalPlanRequestQueue.add(request);
      }
      else {
        WebServicesClient webServicesClient = new WebServicesClient();
        WebResource r = getStramWebResource(webServicesClient, currentApp).path(StramWebServices.PATH_LOGICAL_PLAN_OPERATORS).path(args[1]).path("properties");
        final JSONObject request = new JSONObject();
        request.put(args[2], args[3]);
        JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
          {
            return webResource.accept(MediaType.APPLICATION_JSON).post(JSONObject.class, request);
          }

        });
        printJson(response);
      }
    }

  }

  private class SetPhysicalOperatorPropertyCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      if (currentApp == null) {
        throw new CliException("No application selected");
      }

      WebServicesClient webServicesClient = new WebServicesClient();
      WebResource r = getStramWebResource(webServicesClient, currentApp).path(StramWebServices.PATH_PHYSICAL_PLAN_OPERATORS).path(args[1]).path("properties");
      final JSONObject request = new JSONObject();
      request.put(args[2], args[3]);
      JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
      {
        @Override
        public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
        {
          return webResource.accept(MediaType.APPLICATION_JSON).post(JSONObject.class, request);
        }

      });
      printJson(response);

    }

  }

  private class BeginLogicalPlanChangeCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      changingLogicalPlan = true;
      reader.setHistory(changingLogicalPlanHistory);
    }

  }

  private class ShowLogicalPlanCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String[] newArgs = new String[args.length - 1];
      System.arraycopy(args, 1, newArgs, 0, args.length - 1);
      ShowLogicalPlanCommandLineInfo commandLineInfo = getShowLogicalPlanCommandLineInfo(newArgs);
      Configuration config = StramAppLauncher.getConfig(null, null);
      if (commandLineInfo.libjars != null) {
        commandLineInfo.libjars = expandCommaSeparatedFiles(commandLineInfo.libjars);
        config.set(StramAppLauncher.LIBJARS_CONF_KEY_NAME, commandLineInfo.libjars);
      }

      if (commandLineInfo.args.length >= 2) {
        String jarfile = expandFileName(commandLineInfo.args[0], true);
        String appName = commandLineInfo.args[1];
        StramAppLauncher submitApp = getStramAppLauncher(jarfile, config, commandLineInfo.ignorePom);
        submitApp.loadDependencies();
        List<AppFactory> matchingAppFactories = getMatchingAppFactories(submitApp, appName);
        if (matchingAppFactories == null || matchingAppFactories.isEmpty()) {
          throw new CliException("No application in jar file matches '" + appName + "'");
        }
        else if (matchingAppFactories.size() > 1) {
          throw new CliException("More than one application in jar file match '" + appName + "'");
        }
        else {
          AppFactory appFactory = matchingAppFactories.get(0);
          LogicalPlan logicalPlan = submitApp.prepareDAG(appFactory);
          Map<String, Object> map = new HashMap<String, Object>();
          map.put("applicationName", appFactory.getName());
          map.put("logicalPlan", LogicalPlanSerializer.convertToMap(logicalPlan));
          printJson(map);
        }
      }
      else if (commandLineInfo.args.length == 1) {
        String jarfile = expandFileName(commandLineInfo.args[0], true);
        StramAppLauncher submitApp = getStramAppLauncher(jarfile, config, commandLineInfo.ignorePom);
        submitApp.loadDependencies();
        List<Map<String, Object>> appList = new ArrayList<Map<String, Object>>();
        List<AppFactory> appFactoryList = submitApp.getBundledTopologies();
        for (AppFactory appFactory : appFactoryList) {
          Map<String, Object> m = new HashMap<String, Object>();
          m.put("name", appFactory.getName());
          appList.add(m);
        }
        printJson(appList, "applications");
      }
      else {
        if (currentApp == null) {
          throw new CliException("No application selected");
        }
        WebServicesClient webServicesClient = new WebServicesClient();
        WebResource r = getStramWebResource(webServicesClient, currentApp).path(StramWebServices.PATH_LOGICAL_PLAN);

        JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
          {
            return webResource.accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
          }

        });
        printJson(response);
      }
    }

  }

  private class DumpPropertiesFileCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String outfilename = expandFileName(args[1], false);

      if (args.length > 3) {
        String jarfile = args[2];
        String appName = args[3];
        StramAppLauncher submitApp = getStramAppLauncher(jarfile, null, false);
        submitApp.loadDependencies();
        List<AppFactory> matchingAppFactories = getMatchingAppFactories(submitApp, appName);
        if (matchingAppFactories == null || matchingAppFactories.isEmpty()) {
          throw new CliException("No application in jar file matches '" + appName + "'");
        }
        else if (matchingAppFactories.size() > 1) {
          throw new CliException("More than one application in jar file match '" + appName + "'");
        }
        else {
          AppFactory appFactory = matchingAppFactories.get(0);
          LogicalPlan logicalPlan = submitApp.prepareDAG(appFactory);
          File file = new File(outfilename);
          if (!file.exists()) {
            file.createNewFile();
          }
          LogicalPlanSerializer.convertToProperties(logicalPlan).save(file);
        }
      }
      else {
        if (currentApp == null) {
          throw new CliException("No application selected");
        }
        WebServicesClient webServicesClient = new WebServicesClient();
        WebResource r = getStramWebResource(webServicesClient, currentApp).path(StramWebServices.PATH_LOGICAL_PLAN);

        JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
          {
            return webResource.accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
          }

        });
        File file = new File(outfilename);
        if (!file.exists()) {
          file.createNewFile();
        }
        LogicalPlanSerializer.convertToProperties(response).save(file);
      }
      System.out.println("Property file is saved at " + outfilename);
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

  private class AddStreamSinkCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String streamName = args[1];
      String sinkOperatorName = args[2];
      String sinkPortName = args[3];
      AddStreamSinkRequest request = new AddStreamSinkRequest();
      request.setStreamName(streamName);
      request.setSinkOperatorName(sinkOperatorName);
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
      reader.setHistory(topLevelHistory);
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
      WebResource r = getStramWebResource(webServicesClient, currentApp).path(StramWebServices.PATH_LOGICAL_PLAN);
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
        printJson(response);
      }
      catch (Exception e) {
        throw new CliException("Failed to request " + r.getURI(), e);
      }
      logicalPlanRequestQueue.clear();
      changingLogicalPlan = false;
      reader.setHistory(topLevelHistory);
    }

  }

  private class ShowQueueCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      printJson(logicalPlanRequestQueue, "queue");
      if (consolePresent) {
        System.out.println("Total operations in queue: " + logicalPlanRequestQueue.size());
      }
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
            updateCompleter(reader);
            if (consolePresent) {
              System.out.println("Macro '" + name + "' created.");
            }
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

  private class SetPagerCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      if (args[1].equals("off")) {
        pagerCommand = null;
      }
      else if (args[1].equals("on")) {
        pagerCommand = "less -F -X -r";
      }
      else {
        throw new CliException("set-pager parameter is either on or off.");
      }
    }

  }

  private class GetAppInfoCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      ApplicationReport appReport;
      if (args.length > 1) {
        appReport = getApplication(args[1]);
      }
      else {
        if (currentApp == null) {
          throw new CliException("No application selected");
        }
        appReport = currentApp;
      }
      WebServicesClient webServicesClient = new WebServicesClient();
      WebResource r = getStramWebResource(webServicesClient, appReport).path(StramWebServices.PATH_INFO);

      JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
      {
        @Override
        public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
        {
          return webResource.accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
        }

      });
      printJson(response);
    }

  }

  private class CreateAlertCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String fileName = expandFileName(args[2], true);
      File f = new File(fileName);
      if (!f.canRead()) {
        throw new CliException("Cannot read " + fileName);
      }

      DataInputStream dis = new DataInputStream(new FileInputStream(f));
      byte[] buffer = new byte[dis.available()];
      dis.readFully(buffer);
      final JSONObject json = new JSONObject(new String(buffer));
      dis.close();

      WebServicesClient webServicesClient = new WebServicesClient();
      WebResource r = getStramWebResource(webServicesClient, currentApp).path(StramWebServices.PATH_ALERTS + "/" + args[1]);
      try {
        JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
          {
            return webResource.accept(MediaType.APPLICATION_JSON).put(clazz, json);
          }

        });
        printJson(response);
      }
      catch (Exception e) {
        throw new CliException("Failed to request " + r.getURI(), e);
      }
    }

  }

  private class DeleteAlertCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      WebServicesClient webServicesClient = new WebServicesClient();
      WebResource r = getStramWebResource(webServicesClient, currentApp).path(StramWebServices.PATH_ALERTS + "/" + args[1]);
      try {
        JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
          {
            return webResource.accept(MediaType.APPLICATION_JSON).delete(clazz);
          }

        });
        printJson(response);
      }
      catch (Exception e) {
        throw new CliException("Failed to request " + r.getURI(), e);
      }
    }

  }

  private class ListAlertsCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      ClientResponse rsp = getResource(StramWebServices.PATH_ALERTS, currentApp);
      JSONObject json = rsp.getEntity(JSONObject.class);
      printJson(json);
    }

  }

  @SuppressWarnings("static-access")
  public static Options getLaunchCommandLineOptions()
  {
    Options options = new Options();
    Option local = new Option("local", "Run application in local mode.");
    Option configFile = OptionBuilder.withArgName("configuration file").hasArg().withDescription("Specify an application configuration file.").create("conf");
    Option defProperty = OptionBuilder.withArgName("property=value").hasArg().withDescription("Use value for given property.").create("D");
    Option libjars = OptionBuilder.withArgName("comma separated list of jars").hasArg().withDescription("Specify comma separated jar files to include in the classpath.").create("libjars");
    Option files = OptionBuilder.withArgName("comma separated list of files").hasArg().withDescription("Specify comma separated files to be copied to the cluster.").create("files");
    Option archives = OptionBuilder.withArgName("comma separated list of archives").hasArg().withDescription("Specify comma separated archives to be unarchived on the compute machines.").create("archives");
    Option license = OptionBuilder.withArgName("license file").hasArg().withDescription("Specify the license file to launch the application").create("license");
    Option ignorePom = new Option("ignorepom", "Do not run maven to find the dependency");
    options.addOption(local);
    options.addOption(configFile);
    options.addOption(defProperty);
    options.addOption(libjars);
    options.addOption(files);
    options.addOption(archives);
    options.addOption(license);
    options.addOption(ignorePom);
    return options;
  }

  private static LaunchCommandLineInfo getLaunchCommandLineInfo(String[] args) throws ParseException
  {
    CommandLineParser parser = new PosixParser();
    LaunchCommandLineInfo result = new LaunchCommandLineInfo();
    CommandLine line = parser.parse(getLaunchCommandLineOptions(), args);
    result.localMode = line.hasOption("local");
    result.configFile = line.getOptionValue("conf");
    result.ignorePom = line.hasOption("ignorepom");
    String[] defs = line.getOptionValues("D");
    if (defs != null) {
      result.overrideProperties = new HashMap<String, String>();
      for (String def : defs) {
        int equal = def.indexOf('=');
        if (equal < 0) {
          result.overrideProperties.put(def, null);
        }
        else {
          result.overrideProperties.put(def.substring(0, equal), def.substring(equal + 1));
        }
      }
    }
    result.libjars = line.getOptionValue("libjars");
    result.files = line.getOptionValue("files");
    result.archives = line.getOptionValue("archives");
    result.licenseFile = line.getOptionValue("license");
    result.args = line.getArgs();
    return result;
  }

  private static class LaunchCommandLineInfo
  {
    boolean localMode;
    boolean ignorePom;
    String configFile;
    Map<String, String> overrideProperties;
    String libjars;
    String files;
    String archives;
    String licenseFile;
    String[] args;
  }

  @SuppressWarnings("static-access")
  public static Options getShowLogicalPlanCommandLineOptions()
  {
    Options options = new Options();
    Option libjars = OptionBuilder.withArgName("comma separated list of jars").hasArg().withDescription("Specify comma separated jar files to include in the classpath.").create("libjars");
    Option ignorePom = new Option("ignorepom", "Do not run maven to find the dependency");
    options.addOption(libjars);
    options.addOption(ignorePom);
    return options;
  }

  private static ShowLogicalPlanCommandLineInfo getShowLogicalPlanCommandLineInfo(String[] args) throws ParseException
  {
    CommandLineParser parser = new PosixParser();
    ShowLogicalPlanCommandLineInfo result = new ShowLogicalPlanCommandLineInfo();
    CommandLine line = parser.parse(getShowLogicalPlanCommandLineOptions(), args);
    result.libjars = line.getOptionValue("libjars");
    result.ignorePom = line.hasOption("ignorepom");
    result.args = line.getArgs();
    return result;
  }

  private static class ShowLogicalPlanCommandLineInfo
  {
    String libjars;
    boolean ignorePom;
    String[] args;
  }

  public static void main(String[] args) throws Exception
  {
    DTCli shell = new DTCli();
    shell.init(args);
    shell.run();
    if (lastCommandError) {
      System.exit(1);
    }
  }

}
