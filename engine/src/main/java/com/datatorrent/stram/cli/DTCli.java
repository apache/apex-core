/**
 * Copyright (c) 2012-2013 DataTorrent, Inc. All rights reserved.
 */
package com.datatorrent.stram.cli;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

import javax.ws.rs.core.MediaType;

import jline.console.ConsoleReader;
import jline.console.completer.*;
import jline.console.history.FileHistory;
import jline.console.history.History;
import jline.console.history.MemoryHistory;

import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
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
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import com.google.common.collect.Sets;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.util.JacksonObjectMapperProvider;
import com.datatorrent.stram.StramClient;
import com.datatorrent.stram.client.*;
import com.datatorrent.stram.client.AppPackage.AppInfo;
import com.datatorrent.stram.client.DTConfiguration.Scope;
import com.datatorrent.stram.client.RecordingsAgent.RecordingInfo;
import com.datatorrent.stram.client.StramAppLauncher.AppFactory;
import com.datatorrent.stram.client.StramClientUtils.ClientRMHelper;
import com.datatorrent.stram.client.WebServicesVersionConversion.IncompatibleVersionException;
import com.datatorrent.stram.codec.LogicalPlanSerializer;
import com.datatorrent.stram.license.License;
import com.datatorrent.stram.license.LicenseAuthority;
import com.datatorrent.stram.license.LicenseSection;
import com.datatorrent.stram.license.agent.protocol.LicensingAgentProtocolHelper;
import com.datatorrent.stram.license.agent.protocol.LicensingAgentProtocolHelper.LicensingAgentProtocolInfo;
import com.datatorrent.stram.license.agent.protocol.request.GetMemoryMetricReportRequest;
import com.datatorrent.stram.license.audit.LicenseReport;
import com.datatorrent.stram.license.impl.state.report.ClusterMemoryReportState;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.requests.*;
import com.datatorrent.stram.security.StramUserLogin;
import com.datatorrent.stram.util.VersionInfo;
import com.datatorrent.stram.util.WebServicesClient;
import com.datatorrent.stram.webapp.OperatorDiscoverer;
import com.datatorrent.stram.webapp.StramWebServices;

/**
 * Provides command line interface for a streaming application on hadoop (yarn)
 * <p>
 *
 * @since 0.3.2
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public class DTCli
{
  private static final Logger LOG = LoggerFactory.getLogger(DTCli.class);
  private Configuration conf;
  private FileSystem fs;
  private StramAgent stramAgent;
  private final YarnClient yarnClient = YarnClient.createYarnClient();
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
  private final ObjectMapper mapper = new JacksonObjectMapperProvider().getContext(null);
  private String pagerCommand;
  private Process pagerProcess;
  private int verboseLevel = 0;
  private final Tokenizer tokenizer = new Tokenizer();
  private final Map<String, String> variableMap = new HashMap<String, String>();
  private static boolean lastCommandError = false;
  private Thread mainThread;
  private Thread commandThread;
  private String prompt;
  private String forcePrompt;

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

    @Override
    public String readLine(String prompt, Character mask) throws IOException
    {
      return br.readLine();
    }

    @Override
    public String readLine(Character mask) throws IOException
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

  @SuppressWarnings("unused")
  private StramAppLauncher getStramAppLauncher(String jarfileUri, Configuration config, boolean ignorePom) throws Exception
  {
    URI uri = new URI(jarfileUri);
    String scheme = uri.getScheme();
    StramAppLauncher appLauncher = null;
    if (scheme == null || scheme.equals("file")) {
      File jf = new File(uri.getPath());
      appLauncher = new StramAppLauncher(jf, config);
    }
    else {
      FileSystem tmpFs = FileSystem.newInstance(uri, conf);
      try {
        Path path = new Path(uri.getPath());
        appLauncher = new StramAppLauncher(tmpFs, path, config);
      }
      finally {
        tmpFs.close();
      }
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
      new Arg[]{new CommandArg("command")},
      "Show help"));
    globalCommands.put("connect", new CommandSpec(new ConnectCommand(),
      new Arg[]{new Arg("app-id")},
      null,
      "Connect to an app"));
    globalCommands.put("launch", new OptionsCommandSpec(new LaunchCommand(),
      new Arg[]{new FileArg("jar-file/json-file/properties-file/app-package-file")},
      new Arg[]{new Arg("matching-app-name")},
      "Launch an app", LAUNCH_OPTIONS.options));
    globalCommands.put("shutdown-app", new CommandSpec(new ShutdownAppCommand(),
      new Arg[]{new Arg("app-id")},
      null,
      "Shutdown an app"));
    globalCommands.put("list-apps", new CommandSpec(new ListAppsCommand(),
      null,
      new Arg[]{new Arg("pattern")},
      "List applications"));
    globalCommands.put("kill-app", new CommandSpec(new KillAppCommand(),
      new Arg[]{new Arg("app-id")},
      null,
      "Kill an app"));
    globalCommands.put("show-logical-plan", new OptionsCommandSpec(new ShowLogicalPlanCommand(),
      new Arg[]{new FileArg("jar-file/app-package-file")},
      new Arg[]{new Arg("class-name")},
      "List apps in a jar or show logical plan of an app class",
      getShowLogicalPlanCommandLineOptions()));

    globalCommands.put("get-jar-operator-classes", new OptionsCommandSpec(new GetJarOperatorClassesCommand(),
      new Arg[]{new FileArg("jar-files-comma-separated")},
      new Arg[]{new Arg("search-term")},
      "List operators in a jar list",
      GET_OPERATOR_CLASSES_OPTIONS.options));

    globalCommands.put("get-jar-operator-properties", new CommandSpec(new GetJarOperatorPropertiesCommand(),
      new Arg[]{new FileArg("jar-files-comma-separated"), new Arg("operator-class-name")},
      null,
      "List properties in specified operator"));

    globalCommands.put("alias", new CommandSpec(new AliasCommand(),
      new Arg[]{new Arg("alias-name"), new CommandArg("command")},
      null,
      "Create a command alias"));
    globalCommands.put("source", new CommandSpec(new SourceCommand(),
      new Arg[]{new FileArg("file")},
      null,
      "Execute the commands in a file"));
    globalCommands.put("exit", new CommandSpec(new ExitCommand(),
      null,
      null,
      "Exit the CLI"));
    globalCommands.put("begin-macro", new CommandSpec(new BeginMacroCommand(),
      new Arg[]{new Arg("name")},
      null,
      "Begin Macro Definition ($1...$9 to access parameters and type 'end' to end the definition)"));
    globalCommands.put("dump-properties-file", new CommandSpec(new DumpPropertiesFileCommand(),
      new Arg[]{new FileArg("out-file"), new FileArg("jar-file"), new Arg("class-name")},
      null,
      "Dump the properties file of an app class"));
    globalCommands.put("get-app-info", new CommandSpec(new GetAppInfoCommand(),
      new Arg[]{new Arg("app-id")},
      null,
      "Get the information of an app"));
    globalCommands.put("set-pager", new CommandSpec(new SetPagerCommand(),
      new Arg[]{new Arg("on/off")},
      null,
      "Set the pager program for output"));
    /*
     globalCommands.put("generate-license-request", new CommandSpec(new GenerateLicenseRequestCommand(),
     null,
     null,
     "Generate license request"));
     */
    globalCommands.put("activate-license", new CommandSpec(new ActivateLicenseCommand(),
      null,
      new Arg[]{new FileArg("license-file")},
      "Launch the license agent"));
    globalCommands.put("deactivate-license", new CommandSpec(new DeactivateLicenseCommand(),
      null,
      new Arg[]{new Arg("license-id")},
      "Stop the license agent"));
    globalCommands.put("list-license-agents", new CommandSpec(new ListLicenseAgentsCommand(),
      null,
      null,
      "Show IDs of all license agents"));
    globalCommands.put("show-license-status", new CommandSpec(new ShowLicenseStatusCommand(),
      null,
      new Arg[]{new FileArg("license-file")},
      "Show the status of the license"));
    globalCommands.put("get-config-parameter", new CommandSpec(new GetConfigParameterCommand(),
      null,
      new Arg[]{new FileArg("parameter-name")},
      "Get the configuration parameter"));
    globalCommands.put("get-app-package-info", new CommandSpec(new GetAppPackageInfoCommand(),
      new Arg[]{new FileArg("app-package-file")},
      null,
      "Get info on the app package file"));
    globalCommands.put("get-app-package-operators", new OptionsCommandSpec(new GetAppPackageOperatorsCommand(),
      new Arg[]{new FileArg("app-package-file")},
      new Arg[]{new Arg("search-term")},
      "Get operators within the given app package",
      GET_OPERATOR_CLASSES_OPTIONS.options));
    globalCommands.put("get-app-package-operator-properties", new CommandSpec(new GetAppPackageOperatorPropertiesCommand(),
      new Arg[]{new FileArg("app-package-file"), new Arg("operator-class")},
      null,
      "Get operator properties within the given app package"));
    globalCommands.put("generate-license-report", new CommandSpec(new GenerateLicenseReport(),
      new Arg[]{new Arg("licenseId"), new Arg("month(yyyymm)"), new FileArg("output-file"), new Arg("separator")},
      new Arg[]{new Arg("topNMemoryUsages")},
      "Generate the license report for the given month"));
    //
    // Connected command specification starts here
    //
    connectedCommands.put("list-containers", new CommandSpec(new ListContainersCommand(),
      null,
      null,
      "List containers"));
    connectedCommands.put("list-operators", new CommandSpec(new ListOperatorsCommand(),
      null,
      new Arg[]{new Arg("pattern")},
      "List operators"));
    connectedCommands.put("show-physical-plan", new CommandSpec(new ShowPhysicalPlanCommand(),
      null,
      null,
      "Show physical plan"));
    connectedCommands.put("kill-container", new CommandSpec(new KillContainerCommand(),
      new Arg[]{new Arg("container-id")},
      null,
      "Kill a container"));
    connectedCommands.put("shutdown-app", new CommandSpec(new ShutdownAppCommand(),
      null,
      new Arg[]{new Arg("app-id")},
      "Shutdown an app"));
    connectedCommands.put("kill-app", new CommandSpec(new KillAppCommand(),
      null,
      new Arg[]{new Arg("app-id")},
      "Kill an app"));
    connectedCommands.put("wait", new CommandSpec(new WaitCommand(),
      new Arg[]{new Arg("timeout")},
      null,
      "Wait for completion of current application"));
    connectedCommands.put("start-recording", new CommandSpec(new StartRecordingCommand(),
      new Arg[]{new Arg("operator-id")},
      new Arg[]{new Arg("port-name"), new Arg("num-windows")},
      "Start recording"));
    connectedCommands.put("stop-recording", new CommandSpec(new StopRecordingCommand(),
      new Arg[]{new Arg("operator-id")},
      new Arg[]{new Arg("port-name")},
      "Stop recording"));
    connectedCommands.put("get-operator-attributes", new CommandSpec(new GetOperatorAttributesCommand(),
      new Arg[]{new Arg("operator-name")},
      new Arg[]{new Arg("attribute-name")},
      "Get attributes of an operator"));
    connectedCommands.put("get-operator-properties", new CommandSpec(new GetOperatorPropertiesCommand(),
      new Arg[]{new Arg("operator-name")},
      new Arg[]{new Arg("property-name")},
      "Get properties of an operator"));
    connectedCommands.put("get-physical-operator-properties", new CommandSpec(new GetPhysicalOperatorPropertiesCommand(),
      new Arg[]{new Arg("operator-id")},
      new Arg[]{new Arg("property-name")},
      "Get properties of an operator"));

    connectedCommands.put("set-operator-property", new CommandSpec(new SetOperatorPropertyCommand(),
      new Arg[]{new Arg("operator-name"), new Arg("property-name"), new Arg("property-value")},
      null,
      "Set a property of an operator"));
    connectedCommands.put("set-physical-operator-property", new CommandSpec(new SetPhysicalOperatorPropertyCommand(),
      new Arg[]{new Arg("operator-id"), new Arg("property-name"), new Arg("property-value")},
      null,
      "Set a property of an operator"));
    connectedCommands.put("get-app-attributes", new CommandSpec(new GetAppAttributesCommand(),
      null,
      new Arg[]{new Arg("attribute-name")},
      "Get attributes of the connected app"));
    connectedCommands.put("get-port-attributes", new CommandSpec(new GetPortAttributesCommand(),
      new Arg[]{new Arg("operator-name"), new Arg("port-name")},
      new Arg[]{new Arg("attribute-name")},
      "Get attributes of a port"));
    connectedCommands.put("begin-logical-plan-change", new CommandSpec(new BeginLogicalPlanChangeCommand(),
      null,
      null,
      "Begin Logical Plan Change"));
    connectedCommands.put("show-logical-plan", new OptionsCommandSpec(new ShowLogicalPlanCommand(),
      null,
      new Arg[]{new FileArg("jar-file/app-package-file"), new Arg("class-name")},
      "Show logical plan of an app class",
      getShowLogicalPlanCommandLineOptions()));
    connectedCommands.put("dump-properties-file", new CommandSpec(new DumpPropertiesFileCommand(),
      new Arg[]{new FileArg("out-file")},
      new Arg[]{new FileArg("jar-file"), new Arg("class-name")},
      "Dump the properties file of an app class"));
    connectedCommands.put("get-app-info", new CommandSpec(new GetAppInfoCommand(),
      null,
      new Arg[]{new Arg("app-id")},
      "Get the information of an app"));
    connectedCommands.put("get-recording-info", new CommandSpec(new GetRecordingInfoCommand(),
      null,
      new Arg[]{new Arg("operator-id"), new Arg("start-time")},
      "Get tuple recording info"));

    //
    // Logical plan change command specification starts here
    //
    logicalPlanChangeCommands.put("help", new CommandSpec(new HelpCommand(),
      null,
      new Arg[]{new Arg("command")},
      "Show help"));
    logicalPlanChangeCommands.put("create-operator", new CommandSpec(new CreateOperatorCommand(),
      new Arg[]{new Arg("operator-name"), new Arg("class-name")},
      null,
      "Create an operator"));
    logicalPlanChangeCommands.put("create-stream", new CommandSpec(new CreateStreamCommand(),
      new Arg[]{new Arg("stream-name"), new Arg("from-operator-name"), new Arg("from-port-name"), new Arg("to-operator-name"), new Arg("to-port-name")},
      null,
      "Create a stream"));
    logicalPlanChangeCommands.put("add-stream-sink", new CommandSpec(new AddStreamSinkCommand(),
      new Arg[]{new Arg("stream-name"), new Arg("to-operator-name"), new Arg("to-port-name")},
      null,
      "Add a sink to an existing stream"));
    logicalPlanChangeCommands.put("remove-operator", new CommandSpec(new RemoveOperatorCommand(),
      new Arg[]{new Arg("operator-name")},
      null,
      "Remove an operator"));
    logicalPlanChangeCommands.put("remove-stream", new CommandSpec(new RemoveStreamCommand(),
      new Arg[]{new Arg("stream-name")},
      null,
      "Remove a stream"));
    logicalPlanChangeCommands.put("set-operator-property", new CommandSpec(new SetOperatorPropertyCommand(),
      new Arg[]{new Arg("operator-name"), new Arg("property-name"), new Arg("property-value")},
      null,
      "Set a property of an operator"));
    logicalPlanChangeCommands.put("set-operator-attribute", new CommandSpec(new SetOperatorAttributeCommand(),
      new Arg[]{new Arg("operator-name"), new Arg("attr-name"), new Arg("attr-value")},
      null,
      "Set an attribute of an operator"));
    logicalPlanChangeCommands.put("set-port-attribute", new CommandSpec(new SetPortAttributeCommand(),
      new Arg[]{new Arg("operator-name"), new Arg("port-name"), new Arg("attr-name"), new Arg("attr-value")},
      null,
      "Set an attribute of a port"));
    logicalPlanChangeCommands.put("set-stream-attribute", new CommandSpec(new SetStreamAttributeCommand(),
      new Arg[]{new Arg("stream-name"), new Arg("attr-name"), new Arg("attr-value")},
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
      pagerProcess = Runtime.getRuntime().exec(new String[]{"sh", "-c",
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
      scanner.setIncludes(new String[]{fileName});
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
      return new String[]{fileName};
    }
    if (fileName.startsWith("~" + File.separator)) {
      fileName = System.getProperty("user.home") + fileName.substring(1);
    }
    fileName = new File(fileName).getCanonicalPath();
    LOG.debug("Canonical path: {}", fileName);
    DirectoryScanner scanner = new DirectoryScanner();
    scanner.setIncludes(new String[]{fileName});
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

  public void preImpersonationInit(String[] args) throws IOException
  {
    Signal.handle(new Signal("INT"), new SignalHandler()
    {
      @Override
      public void handle(Signal sig)
      {
        System.out.println("^C");
        if (commandThread != null) {
          commandThread.interrupt();
          mainThread.interrupt();
        }
        else {
          System.out.print(prompt);
          System.out.flush();
        }
      }
    });
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
    options.addOption("f", true, "Use the given prompt at all time");
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
      if (cmd.hasOption("f")) {
        forcePrompt = cmd.getOptionValue("f");
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

    for (org.apache.log4j.Logger logger : new org.apache.log4j.Logger[]{org.apache.log4j.Logger.getRootLogger(),
      org.apache.log4j.Logger.getLogger(DTCli.class)}) {
      @SuppressWarnings("unchecked")
      Enumeration<Appender> allAppenders = logger.getAllAppenders();
      while (allAppenders.hasMoreElements()) {
        Appender appender = allAppenders.nextElement();
        if (appender instanceof ConsoleAppender) {
          ((ConsoleAppender) appender).setThreshold(logLevel);
        }
      }
    }

    if (commandsToExecute != null) {
      for (String command : commandsToExecute) {
        LOG.debug("Command to be executed: {}", command);
      }
    }
  }

  public void init(String[] args) throws IOException
  {
    conf = StramClientUtils.addDTSiteResources(new YarnConfiguration());
    fs = StramClientUtils.newFileSystemInstance(conf);
    stramAgent = new StramAgent(fs, conf);

    // Need to initialize security before starting RPC for the credentials to
    // take effect
    StramUserLogin.attemptAuthentication(conf);
    yarnClient.init(conf);
    yarnClient.start();
    LOG.debug("Yarn Client initialized and started");
    String socks = conf.get(CommonConfigurationKeysPublic.HADOOP_SOCKS_SERVER_KEY);
    if (socks != null) {
      int colon = socks.indexOf(':');
      if (colon > 0) {
        LOG.info("Using socks proxy at {}", socks);
        System.setProperty("socksProxyHost", socks.substring(0, colon));
        System.setProperty("socksProxyPort", socks.substring(colon + 1));
      }
    }
  }

  private void processSourceFile(String fileName, ConsoleReader reader) throws FileNotFoundException, IOException
  {
    fileName = expandFileName(fileName, true);
    LOG.debug("Sourcing {}", fileName);
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
      Arg[] args = (Arg[]) ArrayUtils.addAll(cs.requiredArgs, cs.optionalArgs);
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
              argCompleters.add(new StringsCompleter(commands.keySet().toArray(new String[]{})));
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
    argCompleters.add(new StringsCompleter(set.toArray(new String[]{})));
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
    File historyFile = new File(StramClientUtils.getUserDTDirectory(), "cli_history");
    historyFile.getParentFile().mkdirs();
    try {
      topLevelHistory = new FileHistory(historyFile);
      reader.setHistory(topLevelHistory);
      historyFile = new File(StramClientUtils.getUserDTDirectory(), "cli_history_clp");
      changingLogicalPlanHistory = new FileHistory(historyFile);
    }
    catch (IOException ex) {
      System.err.printf("Unable to open %s for writing.", historyFile);
    }
  }

  private void setupAgents() throws IOException
  {
    recordingsAgent = new RecordingsAgent(stramAgent);
  }

  public void run() throws IOException
  {
    ConsoleReader reader = new ConsoleReader();
    reader.setExpandEvents(false);
    reader.setBellEnabled(false);
    try {
      processSourceFile(StramClientUtils.getConfigDir() + "/clirc_system", reader);
    }
    catch (Exception ex) {
      // ignore
    }
    try {
      processSourceFile(StramClientUtils.getUserDTDirectory() + "/clirc", reader);
    }
    catch (Exception ex) {
      // ignore
    }
    if (consolePresent) {
      printWelcomeMessage();
      //printLicenseStatus();
      setupCompleter(reader);
      setupHistory(reader);
      //reader.setHandleUserInterrupt(true);
    }
    else {
      reader.setEchoCharacter((char) 0);
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

  private void processLine(String line, final ConsoleReader reader, boolean expandMacroAlias)
  {
    try {
      // clear interrupt flag
      Thread.interrupted();
      if (reader.isHistoryEnabled()) {
        History history = reader.getHistory();
        if (history instanceof FileHistory) {
          try {
            ((FileHistory) history).flush();
          }
          catch (IOException ex) {
            // ignore
          }
        }
      }
      //LOG.debug("line: \"{}\"", line);
      List<String[]> commands = tokenizer.tokenize(line);
      if (commands == null) {
        return;
      }
      for (final String[] args : commands) {
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
          final Command command = cs.command;
          commandThread = new Thread()
          {
            @Override
            public void run()
            {
              try {
                command.execute(args, reader);
                lastCommandError = false;
              }
              catch (Exception e) {
                handleException(e);
              }
            }

          };
          mainThread = Thread.currentThread();
          commandThread.start();
          try {
            commandThread.join();
          }
          catch (InterruptedException ex) {
            System.err.println("Interrupted");
          }
          commandThread = null;
        }
      }
    }
    catch (Exception e) {
      handleException(e);
    }
  }

  private void handleException(Exception e)
  {
    String msg = e.getMessage();
    Throwable cause = e.getCause();
    if (cause != null && cause.getMessage() != null) {
      msg += ": " + cause.getMessage();
    }
    if (msg != null) {
      System.err.println(msg);
    }
    LOG.error("Exception caught: ", e);
    lastCommandError = true;
  }

  private void printWelcomeMessage()
  {
    System.out.println("DT CLI " + VersionInfo.getVersion() + " " + VersionInfo.getDate() + " " + VersionInfo.getRevision());
    if (!StramClientUtils.configComplete(conf)) {
      System.err.println("WARNING: Configuration of DataTorrent has not been complete. Please proceed with caution and only in development environment!");
    }
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
      OptionsCommandSpec ocs = (OptionsCommandSpec) commandSpec;
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
      OptionsCommandSpec ocs = (OptionsCommandSpec) commandSpec;
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
    if (forcePrompt == null) {
      prompt = "";
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
    }
    else {
      prompt = forcePrompt;
    }
    String line = reader.readLine(prompt, consolePresent ? null : (char) 0);
    if (line == null) {
      return null;
    }
    return ltrim(line);
  }

  private List<ApplicationReport> getApplicationList()
  {
    try {
      return yarnClient.getApplications(Sets.newHashSet(StramClient.YARN_APPLICATION_TYPE));
    }
    catch (Exception e) {
      throw new CliException("Error getting application list from resource manager", e);
    }
  }

  private List<ApplicationReport> getLicenseAgentList()
  {
    try {
      return yarnClient.getApplications(Sets.newHashSet(StramClient.YARN_APPLICATION_TYPE_LICENSE), EnumSet.of(YarnApplicationState.RUNNING));
    }
    catch (Exception e) {
      throw new CliException("Error getting application list from resource manager", e);
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
        containers = (JSONArray) containersObj;
      }
      else {
        containers = new JSONArray();
        containers.put(containersObj);
      }
      if (containersObj != null) {
        for (int o = containers.length(); o-- > 0; ) {
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
      r = yarnClient.getApplicationReport(app.getApplicationId());
      if (r.getYarnApplicationState() != YarnApplicationState.RUNNING) {
        String msg = String.format("Application %s not running (status %s)",
          r.getApplicationId().getId(), r.getYarnApplicationState());
        throw new CliException(msg);
      }
    }
    catch (YarnException rmExc) {
      throw new CliException("Unable to determine application status", rmExc);
    }
    catch (IOException rmExc) {
      throw new CliException("Unable to determine application status", rmExc);
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
      throw new CliException("Application terminated");
    }

    WebServicesClient wsClient = new WebServicesClient();
    Client client = wsClient.getClient();
    client.setFollowRedirects(true);
    WebResource r;

    try {
      r = stramAgent.getStramWebResource(wsClient, appReport.getApplicationId().toString());
    }
    catch (IncompatibleVersionException ex) {
      throw new CliException("Incompatible stram version", ex);
    }
    if (r == null) {
      throw new CliException("Application " + appReport.getApplicationId().toString() + " has not started");
    }
    r = r.path(resourcePath);

    try {
      return wsClient.process(r.getRequestBuilder(), ClientResponse.class, new WebServicesClient.WebServicesHandler<ClientResponse>()
      {
        @Override
        public ClientResponse process(WebResource.Builder webResource, Class<ClientResponse> clazz)
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
      WebResource wr = stramAgent.getStramWebResource(webServicesClient, appReport.getApplicationId().toString());
      if (wr == null) {
        throw new CliException("Cannot access the application master for this application.");
      }
      return wr;
    }
    catch (IncompatibleVersionException ex) {
      throw new CliException("Incompatible Stram version", ex);
    }
  }

  private List<AppFactory> getMatchingAppFactories(StramAppLauncher submitApp, String matchString, boolean exactMatch)
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
        if (!exactMatch) {
          matchString = matchString.toLowerCase();
        }
        for (AppFactory ac : cfgList) {
          String appName = ac.getName();
          String appAlias = submitApp.getLogicalPlanConfiguration().getAppAlias(appName);
          if (exactMatch) {
            if (matchString.equals(appName) || matchString.equals(appAlias)) {
              result.add(ac);
            }
          }
          else if (appName.toLowerCase().contains(matchString) || (appAlias != null && appAlias.toLowerCase().contains(matchString))) {
            result.add(ac);
          }
        }
        return result;
      }
    }
    catch (Exception ex) {
      LOG.warn("Caught Exception: ", ex);
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
        throw new CliException("Streaming application with id " + args[1] + " is not found.");
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
        file = expandFileName(args[1], true);
      }
      String licenseId = StramClientUtils.activateLicense(file, conf);
      System.out.println("Started license agent for " + licenseId);
    }

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
        licenseId = LicenseAuthority.getLicenseID(licenseBytes);
        LicenseAuthority.validateLicense(licenseBytes);
      }
      ApplicationReport ar = LicensingAgentProtocolHelper.getLicensingAgentAppReport(licenseId, yarnClient);
      if (ar == null) {
        throw new CliException("License not activated: " + licenseId);
      }
      yarnClient.killApplication(ar.getApplicationId());
      System.out.println("Stopped license agent for " + licenseId);
    }

  }

  private static class LicenseInfo
  {
    int remainingLicensedMB;
    int totalLicensedMB;
    // add expiration date range here
  }

  private Map<String, LicenseInfo> getLicenseInfoMap()
  {
    Map<String, LicenseInfo> licenseInfoMap = new HashMap<String, LicenseInfo>();

    int rpcTimeout = 1000;
    List<ApplicationReport> licenseAgentList = getLicenseAgentList();
    for (ApplicationReport licenseAgent : licenseAgentList) {
      String licenseId = licenseAgent.getName();
      try {
        LicensingAgentProtocolInfo lap = LicensingAgentProtocolHelper.getLicensingAgentProtocol(licenseId, conf, rpcTimeout, null);
        ClusterMemoryReportState reportState = lap.protocol.getMemoryMetricReport(new GetMemoryMetricReportRequest()).getReportState();
        LicenseInfo licenseInfo = new LicenseInfo();
        licenseInfo.remainingLicensedMB = reportState.getFreeMemoryMB();
        licenseInfo.totalLicensedMB = reportState.getFreeMemoryMB() + reportState.getUsedMemoryMB();
        licenseInfoMap.put(licenseId, licenseInfo);
      }
      catch (Exception ex) {
        LOG.warn("Cannot get license info for license id {}", licenseId, ex);
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

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");

        for (ApplicationReport ar : licList) {
          JSONObject jsonObj = new JSONObject();
          jsonObj.put("id", ar.getName());
          jsonObj.put("agentAppId", ar.getApplicationId().getId());
          jsonObj.put("startTime", sdf.format(new java.util.Date(ar.getStartTime())));
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
      licenseBytes = StramClientUtils.getLicense(licenseFile, conf);
    }
    else {
      licenseBytes = StramClientUtils.getLicense(conf);
    }
    License license = LicenseAuthority.getLicense(licenseBytes);
    String licenseID = license.getLicenseId();
    LicenseSection licenseSection = license.getLicenseSection();
    JSONObject licenseObj = new JSONObject();
    licenseObj.put("id", licenseID);

    JSONArray sectionArr = new JSONArray();

    SimpleDateFormat sdf = new SimpleDateFormat(LicenseSection.DATE_FORMAT);

    JSONObject sectionObj = new JSONObject();
    sectionObj.put("startDate", sdf.format(licenseSection.getStartDate()));
    sectionObj.put("endDate", sdf.format(licenseSection.getEndDate()));
    sectionObj.put("comment", licenseSection.getComment());
    sectionObj.put("processorList", licenseSection.getProcessor().toJSON());
    sectionObj.put("constraint", licenseSection.getConstraint());
    sectionObj.put("url", licenseSection.getUrl());
    sectionArr.put(sectionObj);

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
      JSONObject licenseObj = getLicenseStatus(args.length > 1 ? expandFileName(args[1], true) : null);
      printJson(licenseObj);
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
      Configuration config = StramAppLauncher.getOverriddenConfig(StramClientUtils.addDTSiteResources(new Configuration()), commandLineInfo.configFile, commandLineInfo.overrideProperties);
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
      if (commandLineInfo.origAppId != null) {
        config.set(StramAppLauncher.ORIGINAL_APP_ID, commandLineInfo.origAppId);
      }
      if (commandLineInfo.licenseFile != null) {
        commandLineInfo.licenseFile = expandFileName(commandLineInfo.licenseFile, true);
      }
      config.set(StramAppLauncher.QUEUE_NAME, commandLineInfo.queue != null ? commandLineInfo.queue : "default");

      String fileName = expandFileName(commandLineInfo.args[0], true);
      StramAppLauncher submitApp;
      AppFactory appFactory = null;
      String matchString = commandLineInfo.args.length >= 2 ? commandLineInfo.args[1] : null;
      if (fileName.endsWith(".json")) {
        File file = new File(fileName);
        submitApp = new StramAppLauncher(file.getName(), config);
        appFactory = new StramAppLauncher.JsonFileAppFactory(file);
        if (matchString != null) {
          LOG.warn("Match string \"{}\" is ignored for launching applications specified in JSON", matchString);
        }
      }
      else if (fileName.endsWith(".properties")) {
        File file = new File(fileName);
        submitApp = new StramAppLauncher(file.getName(), config);
        appFactory = new StramAppLauncher.PropertyFileAppFactory(file);
        if (matchString != null) {
          LOG.warn("Match string \"{}\" is ignored for launching applications specified in properties file", matchString);
        }
      }
      else {
        // see if it's an app package
        AppPackage ap = null;
        try {
          ap = new AppPackage(new File(fileName));
        }
        catch (Exception ex) {
          // fall through, it's not an app package
        }
        finally {
          IOUtils.closeQuietly(ap);
        }
        if (ap != null) {
          new LaunchAppPackageCommand().execute(args, reader);
          return;
        }
        submitApp = getStramAppLauncher(fileName, config, commandLineInfo.ignorePom);
      }
      submitApp.loadDependencies();

      if (commandLineInfo.origAppId != null) {
        // ensure app is not running
        ApplicationReport ar = null;
        try {
          ar = getApplication(commandLineInfo.origAppId);
        }
        catch (Exception e) {
          // application (no longer) in the RM history, does not prevent restart from state in DFS
          LOG.debug("Cannot determine status of application {} {}", commandLineInfo.origAppId, ExceptionUtils.getMessage(e));
        }
        if (ar != null) {
          if (ar.getFinalApplicationStatus() == FinalApplicationStatus.UNDEFINED) {
            throw new CliException("Cannot relaunch non-terminated application: " + commandLineInfo.origAppId + " " + ar.getYarnApplicationState());
          }
          if (appFactory == null && matchString == null) {
            // skip selection if we can match application name from previous run
            List<AppFactory> matchingAppFactories = getMatchingAppFactories(submitApp, ar.getName(), commandLineInfo.exactMatch);
            for (AppFactory af : matchingAppFactories) {
              String appName = submitApp.getLogicalPlanConfiguration().getAppAlias(af.getName());
              if (appName == null) {
                appName = af.getName();
              }
              // limit to exact match
              if (appName.equals(ar.getName())) {
                appFactory = af;
                break;
              }
            }
          }
        }
      }

      if (appFactory == null && matchString != null) {
        // attempt to interpret argument as property file - do we still need it?
        try {
          File file = new File(expandFileName(commandLineInfo.args[1], true));
          if (file.exists()) {
            if (commandLineInfo.args[1].endsWith(".properties")) {
              appFactory = new StramAppLauncher.PropertyFileAppFactory(file);
            }
            else if (commandLineInfo.args[1].endsWith(".json")) {
              appFactory = new StramAppLauncher.JsonFileAppFactory(file);
            }
          }
        }
        catch (Throwable t) {
          // ignore
        }
      }

      if (appFactory == null) {
        List<AppFactory> matchingAppFactories = getMatchingAppFactories(submitApp, matchString, commandLineInfo.exactMatch);
        if (matchingAppFactories == null || matchingAppFactories.isEmpty()) {
          throw new CliException("No applications matching \"" + matchString + "\" bundled in jar.");
        }
        else if (matchingAppFactories.size() == 1) {
          appFactory = matchingAppFactories.get(0);
        }
        else if (matchingAppFactories.size() > 1) {

          //Store the appNames sorted in alphabetical order and their position in matchingAppFactories list
          TreeMap<String, Integer> appNamesInAlphabeticalOrder = new TreeMap<String, Integer>();
          // Display matching applications
          for (int i = 0; i < matchingAppFactories.size(); i++) {
            String appName = matchingAppFactories.get(i).getName();
            String appAlias = submitApp.getLogicalPlanConfiguration().getAppAlias(appName);
            if (appAlias != null) {
              appName = appAlias;
            }
            appNamesInAlphabeticalOrder.put(appName, i);
          }

          //Create a mapping between the app display number and original index at matchingAppFactories
          int index = 1;
          HashMap<Integer, Integer> displayIndexToOriginalUnsortedIndexMap = new HashMap<Integer, Integer>();
          for (Map.Entry<String, Integer> entry : appNamesInAlphabeticalOrder.entrySet()) {
            //Map display number of the app to original unsorted index
            displayIndexToOriginalUnsortedIndexMap.put(index, entry.getValue());

            //Display the app names
            System.out.printf("%3d. %s\n", index++, entry.getKey());
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
            reader.setHandleUserInterrupt(true);
            String optionLine;
            try {
              optionLine = reader.readLine("Choose application: ");
            }
            finally {
              reader.setHandleUserInterrupt(false);
              reader.setHistoryEnabled(useHistory);
              reader.setHistory(previousHistory);
              for (Completer c : completers) {
                reader.addCompleter(c);
              }
            }
            try {
              int option = Integer.parseInt(optionLine);
              if (0 < option && option <= matchingAppFactories.size()) {
                int appIndex = displayIndexToOriginalUnsortedIndexMap.get(option);
                appFactory = matchingAppFactories.get(appIndex);
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
            LOG.info("Using license at {} instead of the one in configuration to launch this application", commandLineInfo.licenseFile);
            licenseBytes = StramClientUtils.getLicense(commandLineInfo.licenseFile, conf);
          }
          else {
            licenseBytes = StramClientUtils.getLicense(conf);
          }
          // This is for suppressing System.out printouts from applications so that the user of CLI will not be confused by those printouts
          PrintStream originalStream = System.out;
          ApplicationId appId = null;
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
            License license = LicenseAuthority.getLicense(licenseBytes);
            String licenseId = license.getLicenseId();
            LOG.info("Using license {}", licenseId);
            ApplicationReport ar = LicensingAgentProtocolHelper.getLicensingAgentAppReport(licenseId, yarnClient);
            if (ar == null) {
              /* SPOI-3161
              try {
                LOG.debug("License agent is not running for {}. Trying to automatically start a license agent.", licenseId);
                activateLicense(commandLineInfo.licenseFile);
                long timeout = System.currentTimeMillis() + TIMEOUT_AFTER_ACTIVATE_LICENSE;
                boolean waitMessagePrinted = false;
                do {
                  Thread.sleep(1000);
                  ar = LicensingAgentProtocolHelper.getLicensingAgentAppReport(licenseId, yarnClient);
                  if (ar == null) {
                    if (!raw && !waitMessagePrinted) {
                      System.out.println("Waiting for license agent to start...");
                      waitMessagePrinted = true;
                    }
                  }
                  else if (waitMessagePrinted) {
                    System.out.println("License agent started.");
                  }
                }
                while (ar == null && System.currentTimeMillis() <= timeout);
              }
              catch (Exception ex) {
                if (license.getLicenseType() == License.LicenseType.EVALUATION) {
                  throw new CliException("Trouble activating license. Please contact <support@datatorrent.com> for help", ex);
                }
                else {
                  LOG.warn("Exception activating license ", ex);
                }
              }
              */
              if (license.getLicenseType() == License.LicenseType.EVALUATION) {
                throw new CliException("License manager not running. Please activate license");
              }
              else {
                LOG.warn("License manager not running. Please activate license");
              }
            }
            appId = submitApp.launchApp(appFactory, licenseBytes);
            currentApp = yarnClient.getApplicationReport(appId);
          }
          finally {
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

  private class GetConfigParameterCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      PrintStream os = getOutputPrintStream();
      if (args.length == 1) {
        Map<String, String> sortedMap = new TreeMap<String, String>();
        for (Map.Entry<String, String> entry : conf) {
          sortedMap.put(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, String> entry : sortedMap.entrySet()) {
          os.println(entry.getKey() + "=" + entry.getValue());
        }
      }
      else {
        String value = conf.get(args[1]);
        if (value != null) {
          os.println(value);
        }
      }
      closeOutputPrintStream(os);
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
          apps = new ApplicationReport[]{currentApp};
        }
      }
      else {
        apps = new ApplicationReport[args.length - 1];
        for (int i = 1; i < args.length; i++) {
          apps[i - 1] = getApplication(args[i]);
          if (apps[i - 1] == null) {
            throw new CliException("Streaming application with id " + args[i] + " is not found.");
          }
        }
      }

      for (ApplicationReport app : apps) {
        WebResource r = getStramWebResource(webServicesClient, app).path(StramWebServices.PATH_SHUTDOWN);
        try {
          JSONObject response = webServicesClient.process(r.getRequestBuilder(), JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
          {
            @Override
            public JSONObject process(WebResource.Builder webResource, Class<JSONObject> clazz)
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
            yarnClient.killApplication(currentApp.getApplicationId());
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
          yarnClient.killApplication(app.getApplicationId());
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
          containers = (JSONArray) containersObj;
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
          for (int o = containers.length(); o-- > 0; ) {
            JSONObject container = containers.getJSONObject(o);
            String id = container.getString("id");
            if (id != null && !id.isEmpty()) {
              for (int argc = args.length; argc-- > 1; ) {
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
          arr = (JSONArray) obj;
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
        printJson(webServicesClient.process(r.getRequestBuilder(), JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource.Builder webResource, Class<JSONObject> clazz)
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
        JSONObject response = webServicesClient.process(r.getRequestBuilder(), JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource.Builder webResource, Class<JSONObject> clazz)
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
        ClientRMHelper clientRMHelper = new ClientRMHelper(yarnClient);
        boolean result = clientRMHelper.waitForCompletion(currentApp.getApplicationId(), cb, timeout * 1000);
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
      long numWindows = 0;
      if (args.length >= 3) {
        port = args[2];
      }
      if (args.length >= 4) {
        numWindows = Long.valueOf(args[3]);
      }
      printJson(recordingsAgent.startRecording(currentApp.getApplicationId().toString(), opId, port, numWindows));
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
        String id = args[2];
        RecordingInfo recordingInfo = recordingsAgent.getRecordingInfo(currentApp.getApplicationId().toString(), opId, id);
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
        JSONObject response = webServicesClient.process(r.getRequestBuilder(), JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource.Builder webResource, Class<JSONObject> clazz)
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
        JSONObject response = webServicesClient.process(r.getRequestBuilder(), JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource.Builder webResource, Class<JSONObject> clazz)
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
        JSONObject response = webServicesClient.process(r.getRequestBuilder(), JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource.Builder webResource, Class<JSONObject> clazz)
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
        JSONObject response = webServicesClient.process(r.getRequestBuilder(), JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource.Builder webResource, Class<JSONObject> clazz)
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
        JSONObject response = webServicesClient.process(r.getRequestBuilder(), JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource.Builder webResource, Class<JSONObject> clazz)
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
        JSONObject response = webServicesClient.process(r.getRequestBuilder(), JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource.Builder webResource, Class<JSONObject> clazz)
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
      JSONObject response = webServicesClient.process(r.getRequestBuilder(), JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
      {
        @Override
        public JSONObject process(WebResource.Builder webResource, Class<JSONObject> clazz)
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
      Configuration config = StramClientUtils.addDTSiteResources(new Configuration());
      if (commandLineInfo.libjars != null) {
        commandLineInfo.libjars = expandCommaSeparatedFiles(commandLineInfo.libjars);
        config.set(StramAppLauncher.LIBJARS_CONF_KEY_NAME, commandLineInfo.libjars);
      }

      if (commandLineInfo.args.length >= 2) {
        String jarfile = expandFileName(commandLineInfo.args[0], true);
        AppPackage ap = null;
        // see if the first argument is actually an app package
        try {
          ap = new AppPackage(new File(jarfile));
        }
        catch (Exception ex) {
          // fall through
        }
        if (ap != null) {
          new ShowLogicalPlanAppPackageCommand().execute(args, reader);
          return;
        }
        String appName = commandLineInfo.args[1];
        StramAppLauncher submitApp = getStramAppLauncher(jarfile, config, commandLineInfo.ignorePom);
        submitApp.loadDependencies();
        List<AppFactory> matchingAppFactories = getMatchingAppFactories(submitApp, appName, commandLineInfo.exactMatch);
        if (matchingAppFactories == null || matchingAppFactories.isEmpty()) {
          throw new CliException("No application in jar file matches '" + appName + "'");
        }
        else if (matchingAppFactories.size() > 1) {
          throw new CliException("More than one application in jar file match '" + appName + "'");
        }
        else {
          Map<String, Object> map = new HashMap<String, Object>();
          PrintStream originalStream = System.out;
          AppFactory appFactory = matchingAppFactories.get(0);
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
            LogicalPlan logicalPlan = appFactory.createApp(submitApp.getLogicalPlanConfiguration());
            map.put("applicationName", appFactory.getName());
            map.put("logicalPlan", LogicalPlanSerializer.convertToMap(logicalPlan));
          }
          finally {
            if (raw) {
              System.setOut(originalStream);
            }
          }
          printJson(map);
        }
      }
      else if (commandLineInfo.args.length == 1) {
        String filename = expandFileName(commandLineInfo.args[0], true);
        if (filename.endsWith(".json")) {
          File file = new File(filename);
          StramAppLauncher submitApp = new StramAppLauncher(file.getName(), config);
          AppFactory appFactory = new StramAppLauncher.JsonFileAppFactory(file);
          LogicalPlan logicalPlan = appFactory.createApp(submitApp.getLogicalPlanConfiguration());
          Map<String, Object> map = new HashMap<String, Object>();
          map.put("applicationName", appFactory.getName());
          map.put("logicalPlan", LogicalPlanSerializer.convertToMap(logicalPlan));
          printJson(map);
        }
        else if (filename.endsWith(".properties")) {
          File file = new File(filename);
          StramAppLauncher submitApp = new StramAppLauncher(file.getName(), config);
          AppFactory appFactory = new StramAppLauncher.PropertyFileAppFactory(file);
          LogicalPlan logicalPlan = appFactory.createApp(submitApp.getLogicalPlanConfiguration());
          Map<String, Object> map = new HashMap<String, Object>();
          map.put("applicationName", appFactory.getName());
          map.put("logicalPlan", LogicalPlanSerializer.convertToMap(logicalPlan));
          printJson(map);
        }
        else {
          StramAppLauncher submitApp = getStramAppLauncher(filename, config, commandLineInfo.ignorePom);
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
      }
      else {
        if (currentApp == null) {
          throw new CliException("No application selected");
        }
        WebServicesClient webServicesClient = new WebServicesClient();
        WebResource r = getStramWebResource(webServicesClient, currentApp).path(StramWebServices.PATH_LOGICAL_PLAN);

        JSONObject response = webServicesClient.process(r.getRequestBuilder(), JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource.Builder webResource, Class<JSONObject> clazz)
          {
            return webResource.accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
          }

        });
        printJson(response);
      }
    }

  }

  private class ShowLogicalPlanAppPackageCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String jarfile = expandFileName(args[1], true);
      AppPackage ap = new AppPackage(new File(jarfile), true);

      List<AppInfo> applications = ap.getApplications();

      if (args.length >= 3) {
        for (AppInfo appInfo : applications) {
          if (args[2].equals(appInfo.name)) {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("applicationName", appInfo.name);
            if (appInfo.dag != null) {
              map.put("logicalPlan", LogicalPlanSerializer.convertToMap(appInfo.dag));
            }
            if (appInfo.error != null) {
              map.put("error", appInfo.error);
            }
            printJson(map);
          }
        }
      }
      else {
        List<Map<String, Object>> appList = new ArrayList<Map<String, Object>>();
        for (AppInfo appInfo : applications) {
          Map<String, Object> m = new HashMap<String, Object>();
          m.put("name", appInfo.name);
          m.put("type", appInfo.type);
          appList.add(m);
        }
        printJson(appList, "applications");
      }
      ap.close();
    }

  }

  private File copyToLocal(String[] files) throws IOException
  {
    File tmpDir = new File("/tmp/datatorrent/" + ManagementFactory.getRuntimeMXBean().getName());
    tmpDir.mkdirs();
    for (int i = 0; i < files.length; i++) {
      try {
        URI uri = new URI(files[i]);
        String scheme = uri.getScheme();
        if (scheme == null || scheme.equals("file")) {
          files[i] = uri.getPath();
        }
        else {
          FileSystem tmpFs = FileSystem.newInstance(uri, conf);
          try {
            Path srcPath = new Path(uri.getPath());
            Path dstPath = new Path(tmpDir.getAbsolutePath(), String.valueOf(i) + srcPath.getName());
            tmpFs.copyToLocalFile(srcPath, dstPath);
            files[i] = dstPath.toUri().getPath();
          }
          finally {
            tmpFs.close();
          }
        }
      }
      catch (URISyntaxException ex) {
        throw new RuntimeException(ex);
      }
    }

    return tmpDir;
  }

  public static class GetOperatorClassesCommandLineOptions
  {
    final Options options = new Options();
    final Option parent = add(new Option("parent", "Specify the parent class for the operators"));

    private Option add(Option opt)
    {
      this.options.addOption(opt);
      return opt;
    }

  }

  private static GetOperatorClassesCommandLineOptions GET_OPERATOR_CLASSES_OPTIONS = new GetOperatorClassesCommandLineOptions();

  private static class GetOperatorClassesCommandLineInfo
  {
    String parent;
    String[] args;
  }

  private static GetOperatorClassesCommandLineInfo getGetOperatorClassesCommandLineInfo(String[] args) throws ParseException
  {
    CommandLineParser parser = new PosixParser();
    GetOperatorClassesCommandLineInfo result = new GetOperatorClassesCommandLineInfo();
    CommandLine line = parser.parse(GET_OPERATOR_CLASSES_OPTIONS.options, args);
    result.parent = line.getOptionValue("parent");
    result.args = line.getArgs();
    return result;
  }

  private class GetJarOperatorClassesCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String[] newArgs = new String[args.length - 1];
      System.arraycopy(args, 1, newArgs, 0, args.length - 1);
      GetOperatorClassesCommandLineInfo commandLineInfo = getGetOperatorClassesCommandLineInfo(newArgs);
      String parentName = commandLineInfo.parent != null ? commandLineInfo.parent : Operator.class.getName();

      String[] jarFiles = expandCommaSeparatedFiles(commandLineInfo.args[0]).split(",");
      File tmpDir = copyToLocal(jarFiles);
      try {
        OperatorDiscoverer operatorDiscoverer = new OperatorDiscoverer(jarFiles);
        String searchTerm = commandLineInfo.args.length > 1 ? commandLineInfo.args[1] : null;
        Set<Class<? extends Operator>> operatorClasses = operatorDiscoverer.getOperatorClasses(parentName, searchTerm);
        JSONObject json = new JSONObject();
        JSONArray arr = new JSONArray();
        JSONObject portClassHier = new JSONObject();

        for (Class<? extends Operator> clazz : operatorClasses) {
          try {
            JSONObject oper = operatorDiscoverer.describeOperator(clazz);

            // add class hier info to portClassHier
            operatorDiscoverer.buildPortClassHier(oper, portClassHier);

            arr.put(oper);
          } catch (Throwable t) {
            // ignore this class
          }
        }
        json.put("operatorClasses", arr);
        json.put("portClassHier", portClassHier);
        printJson(json);
      }
      finally {
        FileUtils.deleteDirectory(tmpDir);
      }
    }

  }

  private class GetJarOperatorPropertiesCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String[] jarFiles = expandCommaSeparatedFiles(args[1]).split(",");
      File tmpDir = copyToLocal(jarFiles);
      try {
        OperatorDiscoverer operatorDiscoverer = new OperatorDiscoverer(jarFiles);
        Class<? extends Operator> operatorClass = operatorDiscoverer.getOperatorClass(args[2]);
        printJson(operatorDiscoverer.describeOperator(operatorClass));
      }
      finally {
        FileUtils.deleteDirectory(tmpDir);
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
        Configuration config = StramClientUtils.addDTSiteResources(new Configuration());
        StramAppLauncher submitApp = getStramAppLauncher(jarfile, config, false);
        submitApp.loadDependencies();
        List<AppFactory> matchingAppFactories = getMatchingAppFactories(submitApp, appName, true);
        if (matchingAppFactories == null || matchingAppFactories.isEmpty()) {
          throw new CliException("No application in jar file matches '" + appName + "'");
        }
        else if (matchingAppFactories.size() > 1) {
          throw new CliException("More than one application in jar file match '" + appName + "'");
        }
        else {
          AppFactory appFactory = matchingAppFactories.get(0);
          LogicalPlan logicalPlan = appFactory.createApp(submitApp.getLogicalPlanConfiguration());
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

        JSONObject response = webServicesClient.process(r.getRequestBuilder(), JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource.Builder webResource, Class<JSONObject> clazz)
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

        JSONObject response = webServicesClient.process(r.getRequestBuilder(), JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource.Builder webResource, Class<JSONObject> clazz)
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
          String line;
          if (consolePresent) {
            line = reader.readLine("macro def (" + name + ") > ");
          }
          else {
            line = reader.readLine("", (char) 0);
          }
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
        if (consolePresent) {
          pagerCommand = "less -F -X -r";
        }
      }
      else {
        throw new CliException("set-pager parameter is either on or off.");
      }
    }

  }

  private class GenerateLicenseReport implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      int length = 5;
      if (args.length > 5) {
        length = Integer.valueOf(args[5]);
      }
      LicenseReport report = new LicenseReport(conf);
      report.dumpReportToFile(args[1], args[2], args[3], args[4], length);
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
        if (appReport == null) {
          throw new CliException("Streaming application with id " + args[1] + " is not found.");
        }
      }
      else {
        if (currentApp == null) {
          throw new CliException("No application selected");
        }
        // refresh the state in currentApp
        currentApp = yarnClient.getApplicationReport(currentApp.getApplicationId());
        appReport = currentApp;
      }
      JSONObject response;
      try {
        WebServicesClient webServicesClient = new WebServicesClient();
        WebResource r = getStramWebResource(webServicesClient, appReport).path(StramWebServices.PATH_INFO);

        response = webServicesClient.process(r.getRequestBuilder(), JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource.Builder webResource, Class<JSONObject> clazz)
          {
            return webResource.accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
          }

        });
      }
      catch (Exception ex) {
        response = new JSONObject();
        response.put("startTime", appReport.getStartTime());
        response.put("id", appReport.getApplicationId().toString());
        response.put("name", appReport.getName());
        response.put("user", appReport.getUser());
      }
      response.put("state", appReport.getYarnApplicationState().name());
      response.put("trackingUrl", appReport.getTrackingUrl());
      response.put("finalStatus", appReport.getFinalApplicationStatus());
      printJson(response);
    }

  }

  private class GetAppPackageInfoCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      AppPackage ap = new AppPackage(new File(expandFileName(args[1], true)), true);
      try {
        JacksonObjectMapperProvider jomp = new JacksonObjectMapperProvider();
        jomp.addSerializer(LogicalPlan.class, new LogicalPlanSerializer());
        printJson(new JSONObject(jomp.getContext(null).writeValueAsString(ap)));
      }
      finally {
        IOUtils.closeQuietly(ap);
      }
    }

  }

  private class LaunchAppPackageCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String[] newArgs = new String[args.length - 1];
      System.arraycopy(args, 1, newArgs, 0, args.length - 1);
      LaunchCommandLineInfo commandLineInfo = getLaunchCommandLineInfo(newArgs);

      AppPackage ap = new AppPackage(new File(expandFileName(commandLineInfo.args[0], true)), true);
      String matchAppName = null;
      if (commandLineInfo.args.length > 1) {
        matchAppName = commandLineInfo.args[1];
      }

      List<AppInfo> applications = new ArrayList<AppInfo>(ap.getApplications());

      if (matchAppName != null) {
        Iterator<AppInfo> it = applications.iterator();
        while (it.hasNext()) {
          AppInfo ai = it.next();
          if ((commandLineInfo.exactMatch && !ai.name.equals(matchAppName))
                  || !ai.name.toLowerCase().matches(".*" + matchAppName.toLowerCase() + ".*")) {
            it.remove();
          }
        }
      }

      AppInfo selectedApp = null;

      if (applications.isEmpty()) {
        throw new CliException("No applications in Application Package" + (matchAppName != null ? " matching \"" + matchAppName + "\"" : ""));
      }
      else if (applications.size() == 1) {
        selectedApp = applications.get(0);
      }
      else {
        //Store the appNames sorted in alphabetical order and their position in matchingAppFactories list
        TreeMap<String, Integer> appNamesInAlphabeticalOrder = new TreeMap<String, Integer>();
        // Display matching applications
        for (int i = 0; i < applications.size(); i++) {
          String appName = applications.get(i).name;
          appNamesInAlphabeticalOrder.put(appName, i);
        }

        //Create a mapping between the app display number and original index at matchingAppFactories
        int index = 1;
        HashMap<Integer, Integer> displayIndexToOriginalUnsortedIndexMap = new HashMap<Integer, Integer>();
        for (Map.Entry<String, Integer> entry : appNamesInAlphabeticalOrder.entrySet()) {
          //Map display number of the app to original unsorted index
          displayIndexToOriginalUnsortedIndexMap.put(index, entry.getValue());

          //Display the app names
          System.out.printf("%3d. %s\n", index++, entry.getKey());
        }

        // Exit if not in interactive mode
        if (!consolePresent) {
          throw new CliException("More than one application in Application Package match '" + matchAppName + "'");
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
          reader.setHandleUserInterrupt(true);
          String optionLine;
          try {
            optionLine = reader.readLine("Choose application: ");
          }
          finally {
            reader.setHandleUserInterrupt(false);
            reader.setHistoryEnabled(useHistory);
            reader.setHistory(previousHistory);
            for (Completer c : completers) {
              reader.addCompleter(c);
            }
          }
          try {
            int option = Integer.parseInt(optionLine);
            if (0 < option && option <= applications.size()) {
              int appIndex = displayIndexToOriginalUnsortedIndexMap.get(option);
              selectedApp = applications.get(appIndex);
            }
          }
          catch (Exception ex) {
            // ignore
          }
        }
      }

      if (selectedApp == null) {
        throw new CliException("No application selected");
      }

      try {
        DTConfiguration launchProperties = getLaunchAppPackageProperties(ap, commandLineInfo);
        String appFile = ap.tempDirectory() + "/app/" + selectedApp.file;

        List<String> launchArgs = new ArrayList<String>();

        launchArgs.add("launch");
        launchArgs.add("-exactMatch");
        List<String> absClassPath = new ArrayList<String>(ap.getClassPath());
        for (int i = 0; i < absClassPath.size(); i++) {
          String path = absClassPath.get(i);
          if (!path.startsWith("/")) {
            absClassPath.set(i, ap.tempDirectory() + "/" + path);
          }
        }
        StringBuilder libjarsVal = new StringBuilder();
        if (!absClassPath.isEmpty() || commandLineInfo.libjars != null) {
          if (!absClassPath.isEmpty()) {
            libjarsVal.append(org.apache.commons.lang3.StringUtils.join(absClassPath, ','));
          }
          if (commandLineInfo.libjars != null) {
            if (libjarsVal.length() > 0) {
              libjarsVal.append(",");
            }
            libjarsVal.append(commandLineInfo.libjars);
          }
        }
        if (appFile.endsWith(".json") || appFile.endsWith(".properties")) {
          if (libjarsVal.length() > 0) {
            libjarsVal.append(",");
          }
          libjarsVal.append(ap.tempDirectory()).append("/app/*.jar");
        }
        if (libjarsVal.length() > 0) {
          launchArgs.add("-libjars");
          launchArgs.add(libjarsVal.toString());
        }

        File launchPropertiesFile = new File(ap.tempDirectory(), "launch.xml");
        launchProperties.writeToFile(launchPropertiesFile, "");
        launchArgs.add("-conf");
        launchArgs.add(launchPropertiesFile.getCanonicalPath());
        if (commandLineInfo.localMode) {
          launchArgs.add("-local");
        }
        if (commandLineInfo.files != null) {
          launchArgs.add("-files");
          launchArgs.add(commandLineInfo.files);
        }
        if (commandLineInfo.archives != null) {
          launchArgs.add("-archives");
          launchArgs.add(commandLineInfo.archives);
        }
        if (commandLineInfo.origAppId != null) {
          launchArgs.add("-originalAppId");
          launchArgs.add(commandLineInfo.origAppId);
        }
        if (commandLineInfo.licenseFile != null) {
          launchArgs.add("-license");
          launchArgs.add(commandLineInfo.licenseFile);
        }
        if (commandLineInfo.queue != null) {
          launchArgs.add("-queue");
          launchArgs.add(commandLineInfo.queue);
        }
        launchArgs.add(appFile);
        if (!appFile.endsWith(".json") && !appFile.endsWith(".properties")) {
          launchArgs.add(selectedApp.name);
        }


        LOG.debug("Launch command: {}", StringUtils.join(launchArgs, " "));
        new LaunchCommand().execute(launchArgs.toArray(new String[]{}), reader);
      }
      finally {
        IOUtils.closeQuietly(ap);
      }
    }

  }

  DTConfiguration getLaunchAppPackageProperties(AppPackage ap, LaunchCommandLineInfo commandLineInfo) throws Exception
  {
    DTConfiguration launchProperties = new DTConfiguration();
    Map<String, String> defaultProperties = ap.getDefaultProperties();
    Set<String> requiredProperties = new TreeSet<String>(ap.getRequiredProperties());

    for (Map.Entry<String, String> entry : defaultProperties.entrySet()) {
      launchProperties.set(entry.getKey(), entry.getValue(), Scope.TRANSIENT, null);
      requiredProperties.remove(entry.getKey());
    }

      // settings specified in the user's environment take precedence over defaults in package.
    // since both are merged into a single -conf option below, apply them on top of the defaults here.
    File confFile = new File(StramClientUtils.getUserDTDirectory(), StramClientUtils.DT_SITE_XML_FILE);
    if (confFile.exists()) {
      Configuration userConf = new Configuration(false);
      userConf.addResource(new Path(confFile.toURI()));
      Iterator<Entry<String, String>> it = userConf.iterator();
      while (it.hasNext()) {
        Entry<String, String> entry = it.next();
        // filter relevant entries
        if (entry.getKey().startsWith(StreamingApplication.DT_PREFIX)) {
          launchProperties.set(entry.getKey(), entry.getValue(), Scope.TRANSIENT, null);
          requiredProperties.remove(entry.getKey());
        }
      }
    }

    if (commandLineInfo.apConfigFile != null) {
      DTConfiguration givenConfig = new DTConfiguration();
      givenConfig.loadFile(new File(ap.tempDirectory() + "/conf/" + commandLineInfo.apConfigFile));
      for (Map.Entry<String, String> entry : givenConfig) {
        launchProperties.set(entry.getKey(), entry.getValue(), Scope.TRANSIENT, null);
        requiredProperties.remove(entry.getKey());
      }
    }
    if (commandLineInfo.configFile != null) {
      DTConfiguration givenConfig = new DTConfiguration();
      givenConfig.loadFile(new File(commandLineInfo.configFile));
      for (Map.Entry<String, String> entry : givenConfig) {
        launchProperties.set(entry.getKey(), entry.getValue(), Scope.TRANSIENT, null);
        requiredProperties.remove(entry.getKey());
      }
    }
    if (commandLineInfo.overrideProperties != null) {
      for (Map.Entry<String, String> entry : commandLineInfo.overrideProperties.entrySet()) {
        launchProperties.set(entry.getKey(), entry.getValue(), Scope.TRANSIENT, null);
        requiredProperties.remove(entry.getKey());
      }
    }

    // now look at whether it is in default configuration
    for (Map.Entry<String, String> entry : conf) {
      if (StringUtils.isNotBlank(entry.getValue())) {
        requiredProperties.remove(entry.getKey());
      }
    }
    if (!requiredProperties.isEmpty()) {
      throw new CliException("Required properties not set: " + StringUtils.join(requiredProperties, ", "));
    }

    StramClientUtils.evalProperties(launchProperties);
    return launchProperties;
  }

  private class GetAppPackageOperatorsCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String[] tmpArgs = new String[args.length - 1];
      System.arraycopy(args, 1, tmpArgs, 0, args.length - 1);
      GetOperatorClassesCommandLineInfo commandLineInfo = getGetOperatorClassesCommandLineInfo(tmpArgs);
      AppPackage ap = new AppPackage(new File(expandFileName(commandLineInfo.args[0], true)), true);
      try {
        List<String> newArgs = new ArrayList<String>();
        List<String> jars = new ArrayList<String>();
        for (String jar : ap.getAppJars()) {
          jars.add(ap.tempDirectory() + "/app/" + jar);
        }
        for (String libJar : ap.getClassPath()) {
          jars.add(ap.tempDirectory() + "/" + libJar);
        }
        newArgs.add("get-jar-operator-classes");
        if (commandLineInfo.parent != null) {
          newArgs.add("-parent");
          newArgs.add(commandLineInfo.parent);
        }
        newArgs.add(StringUtils.join(jars, ","));
        for (int i = 1; i < commandLineInfo.args.length; i++) {
          newArgs.add(commandLineInfo.args[i]);
        }
        LOG.debug("Executing: " + newArgs);
        new GetJarOperatorClassesCommand().execute(newArgs.toArray(new String[]{}), reader);

      }
      finally {
        IOUtils.closeQuietly(ap);
      }
    }

  }

  private class GetAppPackageOperatorPropertiesCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      AppPackage ap = new AppPackage(new File(expandFileName(args[1], true)), true);
      try {
        List<String> newArgs = new ArrayList<String>();
        List<String> jars = new ArrayList<String>();
        for (String jar : ap.getAppJars()) {
          jars.add(ap.tempDirectory() + "/app/" + jar);
        }
        for (String libJar : ap.getClassPath()) {
          jars.add(ap.tempDirectory() + "/" + libJar);
        }
        newArgs.add("get-jar-operator-properties");
        newArgs.add(StringUtils.join(jars, ","));
        newArgs.add(args[2]);
        new GetJarOperatorPropertiesCommand().execute(newArgs.toArray(new String[]{}), reader);
      }
      finally {
        IOUtils.closeQuietly(ap);
      }
    }

  }

  @SuppressWarnings("static-access")
  public static class LaunchCommandLineOptions
  {
    final Options options = new Options();
    final Option local = add(new Option("local", "Run application in local mode."));
    final Option configFile = add(OptionBuilder.withArgName("configuration file").hasArg().withDescription("Specify an application configuration file.").create("conf"));
    final Option apConfigFile = add(OptionBuilder.withArgName("app package configuration file").hasArg().withDescription("Specify an application configuration file within the app package if launching an app package.").create("apconf"));
    final Option defProperty = add(OptionBuilder.withArgName("property=value").hasArg().withDescription("Use value for given property.").create("D"));
    final Option libjars = add(OptionBuilder.withArgName("comma separated list of jars").hasArg().withDescription("Specify comma separated jar files to include in the classpath.").create("libjars"));
    final Option files = add(OptionBuilder.withArgName("comma separated list of files").hasArg().withDescription("Specify comma separated files to be copied to the cluster.").create("files"));
    final Option archives = add(OptionBuilder.withArgName("comma separated list of archives").hasArg().withDescription("Specify comma separated archives to be unarchived on the compute machines.").create("archives"));
    final Option license = add(OptionBuilder.withArgName("license file").hasArg().withDescription("Specify the license file to launch the application").create("license"));
    final Option ignorePom = add(new Option("ignorepom", "Do not run maven to find the dependency"));
    final Option originalAppID = add(OptionBuilder.withArgName("application id").hasArg().withDescription("Specify original application identifier for restart.").create("originalAppId"));
    final Option exactMatch = add(new Option("exactMatch", "Only consider applications with exact app name"));
    final Option queue = add(OptionBuilder.withArgName("queue name").hasArg().withDescription("Specify the queue to launch the application").create("queue"));

    private Option add(Option opt)
    {
      this.options.addOption(opt);
      return opt;
    }

  }

  private static LaunchCommandLineOptions LAUNCH_OPTIONS = new LaunchCommandLineOptions();

  static LaunchCommandLineInfo getLaunchCommandLineInfo(String[] args) throws ParseException
  {
    CommandLineParser parser = new PosixParser();
    LaunchCommandLineInfo result = new LaunchCommandLineInfo();
    CommandLine line = parser.parse(LAUNCH_OPTIONS.options, args);
    result.localMode = line.hasOption(LAUNCH_OPTIONS.local.getOpt());
    result.configFile = line.getOptionValue(LAUNCH_OPTIONS.configFile.getOpt());
    result.apConfigFile = line.getOptionValue(LAUNCH_OPTIONS.apConfigFile.getOpt());
    result.ignorePom = line.hasOption(LAUNCH_OPTIONS.ignorePom.getOpt());
    String[] defs = line.getOptionValues(LAUNCH_OPTIONS.defProperty.getOpt());
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
    result.libjars = line.getOptionValue(LAUNCH_OPTIONS.libjars.getOpt());
    result.files = line.getOptionValue(LAUNCH_OPTIONS.files.getOpt());
    result.archives = line.getOptionValue(LAUNCH_OPTIONS.archives.getOpt());
    result.licenseFile = line.getOptionValue(LAUNCH_OPTIONS.license.getOpt());
    result.queue = line.getOptionValue(LAUNCH_OPTIONS.queue.getOpt());
    result.args = line.getArgs();
    result.origAppId = line.getOptionValue(LAUNCH_OPTIONS.originalAppID.getOpt());
    result.exactMatch = line.hasOption("exactMatch");
    return result;
  }

  static class LaunchCommandLineInfo
  {
    boolean localMode;
    boolean ignorePom;
    String configFile;
    String apConfigFile;
    Map<String, String> overrideProperties;
    String libjars;
    String files;
    String queue;
    String archives;
    String licenseFile;
    String origAppId;
    boolean exactMatch;
    String[] args;
  }

  @SuppressWarnings("static-access")
  public static Options getShowLogicalPlanCommandLineOptions()
  {
    Options options = new Options();
    Option libjars = OptionBuilder.withArgName("comma separated list of jars").hasArg().withDescription("Specify comma separated jar files to include in the classpath.").create("libjars");
    Option ignorePom = new Option("ignorepom", "Do not run maven to find the dependency");
    Option exactMatch = new Option("exactMatch", "Only consider exact match for app name");
    options.addOption(libjars);
    options.addOption(ignorePom);
    options.addOption(exactMatch);
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
    result.exactMatch = line.hasOption("exactMatch");
    return result;
  }

  private static class ShowLogicalPlanCommandLineInfo
  {
    String libjars;
    boolean ignorePom;
    String[] args;
    boolean exactMatch;
  }

  public void mainHelper(String[] args) throws Exception
  {
    init(args);
    run();
    System.exit(lastCommandError ? 1 : 0);
  }

  public static void main(final String[] args) throws Exception
  {
    final DTCli shell = new DTCli();
    shell.preImpersonationInit(args);
    String hadoopUserName = System.getenv("HADOOP_USER_NAME");
    if (UserGroupInformation.isSecurityEnabled()
            && StringUtils.isNotBlank(hadoopUserName)
            && !hadoopUserName.equals(UserGroupInformation.getLoginUser().getShortUserName())) {
      LOG.info("You ({}) are running as user {}", UserGroupInformation.getLoginUser().getShortUserName(), hadoopUserName);
      UserGroupInformation ugi
              = UserGroupInformation.createProxyUser(hadoopUserName, UserGroupInformation.getLoginUser());
      ugi.doAs(new PrivilegedExceptionAction<Void>()
      {
        @Override
        public Void run() throws Exception
        {
          shell.mainHelper(args);
          return null;
        }
      });
    }
    else {
      shell.mainHelper(args);
    }
  }

}
