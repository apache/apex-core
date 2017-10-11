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
package com.datatorrent.stram.cli;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.MediaType;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang.math.NumberUtils;
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

import com.google.common.base.Preconditions;
import com.sun.jersey.api.client.WebResource;

import com.datatorrent.api.DAG.GenericOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.StramUtils;
import com.datatorrent.stram.client.AppPackage;
import com.datatorrent.stram.client.AppPackage.AppInfo;
import com.datatorrent.stram.client.AppPackage.PropertyInfo;
import com.datatorrent.stram.client.ConfigPackage;
import com.datatorrent.stram.client.DTConfiguration;
import com.datatorrent.stram.client.DTConfiguration.Scope;
import com.datatorrent.stram.client.RecordingsAgent;
import com.datatorrent.stram.client.RecordingsAgent.RecordingInfo;
import com.datatorrent.stram.client.StramAgent;
import com.datatorrent.stram.client.StramAppLauncher;
import com.datatorrent.stram.client.StramAppLauncher.AppFactory;
import com.datatorrent.stram.client.StramClientUtils;
import com.datatorrent.stram.client.StramClientUtils.ClientRMHelper;
import com.datatorrent.stram.codec.LogicalPlanSerializer;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;
import com.datatorrent.stram.plan.logical.requests.AddStreamSinkRequest;
import com.datatorrent.stram.plan.logical.requests.CreateOperatorRequest;
import com.datatorrent.stram.plan.logical.requests.CreateStreamRequest;
import com.datatorrent.stram.plan.logical.requests.LogicalPlanRequest;
import com.datatorrent.stram.plan.logical.requests.RemoveOperatorRequest;
import com.datatorrent.stram.plan.logical.requests.RemoveStreamRequest;
import com.datatorrent.stram.plan.logical.requests.SetOperatorAttributeRequest;
import com.datatorrent.stram.plan.logical.requests.SetOperatorPropertyRequest;
import com.datatorrent.stram.plan.logical.requests.SetPortAttributeRequest;
import com.datatorrent.stram.plan.logical.requests.SetStreamAttributeRequest;
import com.datatorrent.stram.security.StramUserLogin;
import com.datatorrent.stram.util.JSONSerializationProvider;
import com.datatorrent.stram.util.LoggerUtil;
import com.datatorrent.stram.util.SecurityUtils;
import com.datatorrent.stram.util.VersionInfo;
import com.datatorrent.stram.util.WebServicesClient;
import com.datatorrent.stram.webapp.OperatorDiscoverer;
import com.datatorrent.stram.webapp.StramWebServices;
import com.datatorrent.stram.webapp.TypeDiscoverer;

import jline.console.ConsoleReader;
import jline.console.completer.AggregateCompleter;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.Completer;
import jline.console.completer.FileNameCompleter;
import jline.console.completer.StringsCompleter;
import jline.console.history.FileHistory;
import jline.console.history.History;
import jline.console.history.MemoryHistory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * Provides command line interface for a streaming application on hadoop (yarn)
 * <p>
 *
 * @since 0.3.2
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public class ApexCli
{
  private static final Logger LOG = LoggerFactory.getLogger(ApexCli.class);
  private static String CONFIG_EXCLUSIVE = "exclusive";
  private static String CONFIG_INCLUSIVE = "inclusive";

  private static final String COLOR_RED = "\033[38;5;196m";
  private static final String COLOR_YELLOW = "\033[0;93m";
  private static final String FORMAT_BOLD = "\033[1m";

  private static final String COLOR_RESET = "\033[0m";
  private static final String ITALICS = "\033[3m";
  private static final String APEX_HIGHLIGHT_COLOR_PROPERTY_NAME = "apex.cli.color.highlight";
  private static final String APEX_HIGHLIGHT_COLOR_ENV_VAR_NAME = "APEX_HIGHLIGHT_COLOR";

  protected Configuration conf;
  private FileSystem fs;
  private StramAgent stramAgent;
  private YarnClient yarnClient = null;
  private ApplicationReport currentApp = null;
  private boolean consolePresent;
  private String[] commandsToExecute;
  private final Map<String, CommandSpec> globalCommands = new TreeMap<>();
  private final Map<String, CommandSpec> connectedCommands = new TreeMap<>();
  private final Map<String, CommandSpec> logicalPlanChangeCommands = new TreeMap<>();
  private final Map<String, String> aliases = new HashMap<>();
  private final Map<String, List<String>> macros = new HashMap<>();
  private boolean changingLogicalPlan = false;
  private final List<LogicalPlanRequest> logicalPlanRequestQueue = new ArrayList<>();
  private FileHistory topLevelHistory;
  private FileHistory changingLogicalPlanHistory;
  private String jsonp;
  private boolean raw = false;
  private RecordingsAgent recordingsAgent;
  private final ObjectMapper mapper = new JSONSerializationProvider().getContext(null);
  private String pagerCommand;
  private Process pagerProcess;
  private int verboseLevel = 0;
  private final Tokenizer tokenizer = new Tokenizer();
  private final Map<String, String> variableMap = new HashMap<>();
  private static boolean lastCommandError = false;
  private Thread mainThread;
  private Thread commandThread;
  private String prompt;
  private String forcePrompt;
  private String kerberosPrincipal;
  private String kerberosKeyTab;
  private String highlightColor = null;

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
      List<String> newCommand = new ArrayList<>();
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
      LinkedList<List<String>> resultBuffer = new LinkedList<>();
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
          } else if (c == '\\') {
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
          } else {
            if (insideQuotes) {
              buf.append(c);
            } else {

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
                  } else if (commandLine.charAt(i + 1) == '?') {
                    ++i;
                    buf.append(lastCommandError ? "1" : "0");
                    continue;
                  } else {
                    while (len > i + 1) {
                      char ch = commandLine.charAt(i + 1);
                      if ((variableName.length() > 0 && ch >= '0' && ch <= '9') || ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z'))) {
                        variableName.append(ch);
                      } else {
                        break;
                      }
                      ++i;
                    }
                  }
                  if (variableName.length() == 0) {
                    buf.append(c);
                  } else {
                    String value = variableMap.get(variableName.toString());
                    if (value != null) {
                      buf.append(value);
                    }
                  }
                } else {
                  buf.append(c);
                }
              } else if (c == ';') {
                appendToCommandBuffer(commandBuffer, buf, potentialEmptyArg);
                commandBuffer = startNewCommand(resultBuffer);
              } else if (Character.isWhitespace(c)) {
                appendToCommandBuffer(commandBuffer, buf, potentialEmptyArg);
                potentialEmptyArg = false;
                if (len > i + 1 && commandLine.charAt(i + 1) == '#') {
                  break;
                }
              } else {
                buf.append(c);
              }
            }
          }
        }
        appendToCommandBuffer(commandBuffer, buf, potentialEmptyArg);
      }
      startNewCommand(resultBuffer);
      List<String[]> result = new ArrayList<>();
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

  // VarArg must be in optional argument and must be at the end
  private static class VarArg extends Arg
  {
    VarArg(String name)
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

  protected PrintStream suppressOutput()
  {
    PrintStream originalStream = System.out;
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
    return originalStream;
  }

  protected void restoreOutput(PrintStream originalStream)
  {
    if (raw) {
      System.setOut(originalStream);
    }
  }

  AppPackage newAppPackageInstance(URI uri, boolean suppressOutput) throws IOException
  {
    PrintStream outputStream = suppressOutput ? suppressOutput() : null;
    try {
      final String scheme = uri.getScheme();
      if (scheme == null || scheme.equals("file")) {
        return new AppPackage(new FileInputStream(new File(expandFileName(uri.getPath(), true))), true);
      } else {
        try (FileSystem fs = FileSystem.newInstance(uri, conf)) {
          return new AppPackage(fs.open(new Path(uri.getPath())), true);
        }
      }
    } finally {
      if (outputStream != null) {
        restoreOutput(outputStream);
      }
    }
  }

  AppPackage newAppPackageInstance(File f) throws IOException
  {
    PrintStream outputStream = suppressOutput();
    try {
      return new AppPackage(f, true);
    } finally {
      restoreOutput(outputStream);
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
    } else {
      try (FileSystem tmpFs = FileSystem.newInstance(uri, conf)) {
        Path path = new Path(uri.getPath());
        appLauncher = new StramAppLauncher(tmpFs, path, config);
      }
    }
    if (appLauncher != null) {
      if (verboseLevel > 0) {
        System.err.print(appLauncher.getMvnBuildClasspathOutput());
      }
      return appLauncher;
    } else {
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
        for (Arg arg : optionalArgs) {
          if (arg instanceof VarArg) {
            maxArgs = Integer.MAX_VALUE;
            break;
          } else {
            maxArgs++;
          }
        }
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
          if (arg instanceof VarArg) {
            System.err.print(" [<" + arg + "> ... ]");
          } else {
            System.err.print(" [<" + arg + ">]");
          }
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
      } catch (Exception ex) {
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

  ApexCli()
  {
    //
    // Global command specification starts here
    //
    globalCommands.put("help", new CommandSpec(new HelpCommand(),
        null,
        new Arg[]{new CommandArg("command")},
        "Show help"));
    globalCommands.put("echo", new CommandSpec(new EchoCommand(),
        null, new Arg[]{new VarArg("arg")},
        "Echo the arguments"));
    globalCommands.put("connect", new CommandSpec(new ConnectCommand(),
        new Arg[]{new Arg("app-id")},
        null,
        "Connect to an app"));
    globalCommands.put("launch", new OptionsCommandSpec(new LaunchCommand(),
        new Arg[]{},
        new Arg[]{new FileArg("jar-file/json-file/properties-file/app-package-file-path/app-package-file-uri"), new Arg("matching-app-name")},
        "Launch an app", LAUNCH_OPTIONS.options));
    globalCommands.put("shutdown-app", new CommandSpec(new ShutdownAppCommand(),
        new Arg[]{new Arg("app-id/app-name")},
        new Arg[]{new VarArg("app-id/app-name")},
        "Shutdown application(s) by id or name"));
    globalCommands.put("list-apps", new CommandSpec(new ListAppsCommand(),
        null,
        new Arg[]{new Arg("pattern")},
        "List applications"));
    globalCommands.put("kill-app", new CommandSpec(new KillAppCommand(),
        new Arg[]{new Arg("app-id/app-name")},
        new Arg[]{new VarArg("app-id/app-name")},
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
        new Arg[]{new FileArg("out-file"), new FileArg("jar-file"), new Arg("app-name")},
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
    globalCommands.put("get-config-parameter", new CommandSpec(new GetConfigParameterCommand(),
        null,
        new Arg[]{new FileArg("parameter-name")},
        "Get the configuration parameter"));
    globalCommands.put("get-app-package-info", new OptionsCommandSpec(new GetAppPackageInfoCommand(),
        new Arg[]{new FileArg("app-package-file-path/app-package-file-uri")},
        new Arg[]{new Arg("-withDescription")},
        "Get info on the app package file",
        GET_APP_PACKAGE_INFO_OPTIONS));
    globalCommands.put("get-app-package-operators", new OptionsCommandSpec(new GetAppPackageOperatorsCommand(),
        new Arg[]{new FileArg("app-package-file-path/app-package-file-uri")},
        new Arg[]{new Arg("search-term")},
        "Get operators within the given app package",
        GET_OPERATOR_CLASSES_OPTIONS.options));
    globalCommands.put("get-app-package-operator-properties", new CommandSpec(new GetAppPackageOperatorPropertiesCommand(),
        new Arg[]{new FileArg("app-package-file-path/app-package-file-uri"), new Arg("operator-class")},
        null,
        "Get operator properties within the given app package"));
    globalCommands.put("list-default-app-attributes", new CommandSpec(new ListDefaultAttributesCommand(AttributesType.APPLICATION),
        null, null, "Lists the default application attributes"));
    globalCommands.put("list-default-operator-attributes", new CommandSpec(new ListDefaultAttributesCommand(AttributesType.OPERATOR),
        null, null, "Lists the default operator attributes"));
    globalCommands.put("list-default-port-attributes", new CommandSpec(new ListDefaultAttributesCommand(AttributesType.PORT),
        null, null, "Lists the default port attributes"));
    globalCommands.put("clean-app-directories", new CommandSpec(new CleanAppDirectoriesCommand(),
        new Arg[]{new Arg("duration-in-millis")},
        null,
        "Clean up data directories of applications that terminated the given milliseconds ago"));

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
        new Arg[]{new VarArg("container-id")},
        "Kill a container"));
    connectedCommands.put("shutdown-app", new CommandSpec(new ShutdownAppCommand(),
        null,
        new Arg[]{new VarArg("app-id/app-name")},
        "Shutdown an app"));
    connectedCommands.put("kill-app", new CommandSpec(new KillAppCommand(),
        null,
        new Arg[]{new VarArg("app-id/app-name")},
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
        "Get properties of a logical operator"));
    connectedCommands.put("get-physical-operator-properties", new OptionsCommandSpec(new GetPhysicalOperatorPropertiesCommand(),
        new Arg[]{new Arg("operator-id")},
        null,
        "Get properties of a physical operator", GET_PHYSICAL_PROPERTY_OPTIONS.options));

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
        new Arg[]{new FileArg("jar-file/app-package-file-path/app-package-file-uri"), new Arg("class-name")},
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
    connectedCommands.put("get-container-stacktrace", new CommandSpec(new GetContainerStackTrace(),
        null,
        new Arg[]{new Arg("container-id")},
        "Get the stack trace for the container"));
    connectedCommands.put("set-log-level", new CommandSpec(new SetLogLevelCommand(),
        new Arg[]{new Arg("target"), new Arg("logLevel")},
        null,
        "Set the logging level of any package or class of the connected app instance"));

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
    } else {
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
    } else {
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
      } catch (InterruptedException ex) {
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
      } else if (files.length > 1) {
        throw new CliException(fileName + " matches more than one file");
      }
      return files[0];
    } else {
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
    if (result.length() == 0) {
      return null;
    }
    return result.toString();
  }

  protected ApplicationReport getApplicationByName(String appName)
  {
    if (appName == null) {
      throw new CliException("Invalid application name provided by user");
    }
    List<ApplicationReport> appList = getApplicationList();
    for (ApplicationReport ar : appList) {
      if ((ar.getName().equals(appName)) &&
          (ar.getYarnApplicationState() != YarnApplicationState.KILLED) &&
          (ar.getYarnApplicationState() != YarnApplicationState.FINISHED)) {
        LOG.debug("Application Name: {} Application ID: {} Application State: {}",
            ar.getName(), ar.getApplicationId().toString(), YarnApplicationState.FINISHED);
        return ar;
      }
    }
    return null;
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
    } else {
      for (ApplicationReport ar : appList) {
        if (ar.getApplicationId().toString().equals(appId)) {
          return ar;
        }
      }
    }
    return null;
  }

  static class CliException extends RuntimeException
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
        } else {
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
    options.addOption("f", true, "Use the specified prompt at all time");
    options.addOption("kp", true, "Use the specified kerberos principal");
    options.addOption("kt", true, "Use the specified kerberos keytab");

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
        formatter.printHelp(ApexCli.class.getSimpleName(), options);
        System.exit(0);
      }
      if (cmd.hasOption("kp")) {
        kerberosPrincipal = cmd.getOptionValue("kp");
      }
      if (cmd.hasOption("kt")) {
        kerberosKeyTab = cmd.getOptionValue("kt");
      }
    } catch (ParseException ex) {
      System.err.println("Invalid argument: " + ex);
      System.exit(1);
    }

    if (kerberosPrincipal == null && kerberosKeyTab != null) {
      System.err.println("Kerberos key tab is specified but not the kerberos principal. Please specify it using the -kp option.");
      System.exit(1);
    }
    if (kerberosPrincipal != null && kerberosKeyTab == null) {
      System.err.println("Kerberos principal is specified but not the kerberos key tab. Please specify it using the -kt option.");
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

    for (org.apache.log4j.Logger logger : new org.apache.log4j.Logger[]{
        org.apache.log4j.Logger.getRootLogger(),
        org.apache.log4j.Logger.getLogger(ApexCli.class)
    }) {

     /*
      * Override logLevel specified by user, the same logLevel would be inherited by all
      * appenders related to logger.
      */
      logger.setLevel(logLevel);

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
    if (kerberosPrincipal != null && kerberosKeyTab != null) {
      StramUserLogin.authenticate(kerberosPrincipal, kerberosKeyTab);
    } else {
      Configuration config = new YarnConfiguration();
      StramClientUtils.addDTLocalResources(config);
      StramUserLogin.attemptAuthentication(config);
    }
  }

  /**
   * get highlight color based on env variable first and then config
   *
   */
  protected String getHighlightColor()
  {
    if (highlightColor == null) {
      highlightColor = System.getenv(APEX_HIGHLIGHT_COLOR_ENV_VAR_NAME);
      if (StringUtils.isBlank(highlightColor)) {
        highlightColor = conf.get(APEX_HIGHLIGHT_COLOR_PROPERTY_NAME, FORMAT_BOLD);
      }
      highlightColor = highlightColor.replace("\\e", "\033");
    }
    return highlightColor;
  }

  public void init() throws IOException
  {
    conf = StramClientUtils.addDTSiteResources(new YarnConfiguration());
    SecurityUtils.init(conf);
    fs = StramClientUtils.newFileSystemInstance(conf);
    stramAgent = new StramAgent(fs, conf);

    yarnClient = StramClientUtils.createYarnClient(conf);
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
    } finally {
      consolePresent = consolePresentSaved;
      if (fr != null) {
        fr.close();
      }
    }
  }

  private static final class MyNullCompleter implements Completer
  {
    public static final MyNullCompleter INSTANCE = new MyNullCompleter();

    @Override
    public int complete(final String buffer, final int cursor, final List<CharSequence> candidates)
    {
      candidates.add("");
      return cursor;
    }

  }

  private static final class MyFileNameCompleter extends FileNameCompleter
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
    Map<String, CommandSpec> commands = new TreeMap<>();

    commands.putAll(logicalPlanChangeCommands);
    commands.putAll(connectedCommands);
    commands.putAll(globalCommands);

    List<Completer> completers = new LinkedList<>();
    for (Map.Entry<String, CommandSpec> entry : commands.entrySet()) {
      String command = entry.getKey();
      CommandSpec cs = entry.getValue();
      List<Completer> argCompleters = new LinkedList<>();
      argCompleters.add(new StringsCompleter(command));
      Arg[] args = (Arg[])ArrayUtils.addAll(cs.requiredArgs, cs.optionalArgs);
      if (args != null) {
        if (cs instanceof OptionsCommandSpec) {
          // ugly hack because jline cannot dynamically change completer while user types
          if (args[0] instanceof FileArg || args[0] instanceof VarArg) {
            for (int i = 0; i < 10; i++) {
              argCompleters.add(new MyFileNameCompleter());
            }
          }
        } else {
          for (Arg arg : args) {
            if (arg instanceof FileArg || arg instanceof VarArg) {
              argCompleters.add(new MyFileNameCompleter());
            } else if (arg instanceof CommandArg) {
              argCompleters.add(new StringsCompleter(commands.keySet().toArray(new String[]{})));
            } else {
              argCompleters.add(MyNullCompleter.INSTANCE);
            }
          }
        }
      }

      completers.add(new ArgumentCompleter(argCompleters));
    }

    List<Completer> argCompleters = new LinkedList<>();
    Set<String> set = new TreeSet<>();
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
    List<Completer> completers = new ArrayList<>(reader.getCompleters());
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
    } catch (IOException ex) {
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
    } catch (Exception ex) {
      // ignore
    }
    try {
      processSourceFile(StramClientUtils.getUserDTDirectory() + "/clirc", reader);
    } catch (Exception ex) {
      // ignore
    }
    if (consolePresent) {
      printWelcomeMessage();
      setupCompleter(reader);
      setupHistory(reader);
      //reader.setHandleUserInterrupt(true);
    } else {
      reader.setEchoCharacter((char)0);
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
      } else {
        line = readLine(reader);
        if (line == null) {
          break;
        }
      }
      processLine(line, reader, true);
      out.flush();
    }
    if (topLevelHistory != null) {
      try {
        topLevelHistory.flush();
      } catch (IOException ex) {
        LOG.warn("Cannot flush command history", ex);
      }
    }
    if (changingLogicalPlanHistory != null) {
      try {
        changingLogicalPlanHistory.flush();
      } catch (IOException ex) {
        LOG.warn("Cannot flush command history", ex);
      }
    }
    if (consolePresent) {
      System.out.println("exit");
    }
  }

  private List<String> expandMacro(List<String> lines, String[] args)
  {
    List<String> expandedLines = new ArrayList<>();

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
          } else if (argIndex >= 0 && argIndex <= 9) {
            // Arguments for $1..$9 were not supplied - replace with empty strings
            expandedLine.append(line.substring(previousIndex, currentIndex));
          } else {
            // Outside valid arguments range - ignore and do not replace
            expandedLine.append(line.substring(previousIndex, currentIndex + 2));
          }
          currentIndex += 2;
        } else {
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

  protected void processLine(String line, final ConsoleReader reader, boolean expandMacroAlias)
  {
    try {
      // clear interrupt flag
      Thread.interrupted();
      if (reader.isHistoryEnabled()) {
        History history = reader.getHistory();
        if (history instanceof FileHistory) {
          try {
            ((FileHistory)history).flush();
          } catch (IOException ex) {
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
        } else {
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
            lastCommandError = true;
          } else if (logicalPlanChangeCommands.get(args[0]) != null) {
            System.err.println("\"" + args[0] + "\" is valid only when changing a logical plan.  Type \"begin-logical-plan-change\" to change a logical plan");
            lastCommandError = true;
          } else {
            System.err.println("Invalid command '" + args[0] + "'. Type \"help\" for list of commands");
            lastCommandError = true;
          }
        } else {
          try {
            cs.verifyArguments(args);
          } catch (CliException ex) {
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
              } catch (Exception e) {
                handleException(e);
              } catch (Error e) {
                handleException(e);
                System.err.println("Fatal error encountered");
                System.exit(1);
              }
            }

          };
          mainThread = Thread.currentThread();
          commandThread.start();
          try {
            commandThread.join();
          } catch (InterruptedException ex) {
            System.err.println("Interrupted");
          }
          commandThread = null;
        }
      }
    } catch (Exception e) {
      handleException(e);
    }
  }

  private void handleException(Throwable e)
  {
    System.err.println(ExceptionUtils.getFullStackTrace(e));
    LOG.error("Exception caught: ", e);
    lastCommandError = true;
  }

  private void printWelcomeMessage()
  {
    VersionInfo v = VersionInfo.APEX_VERSION;
    System.out.println("Apex CLI " + v.getVersion() + " " + v.getDate() + " " + v.getRevision());
  }

  private void printHelp(String command, CommandSpec commandSpec, PrintStream os)
  {
    if (consolePresent) {
      os.print(getHighlightColor());
      os.print(command);
      os.print(COLOR_RESET);
    } else {
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
          os.print(" " + ITALICS + arg + COLOR_RESET);
        } else {
          os.print(" <" + arg + ">");
        }
      }
    }
    if (commandSpec.optionalArgs != null) {
      for (Arg arg : commandSpec.optionalArgs) {
        if (consolePresent) {
          os.print(" [" + ITALICS + arg + COLOR_RESET);
        } else {
          os.print(" [<" + arg + ">");
        }
        if (arg instanceof VarArg) {
          os.print(" ...");
        }
        os.print("]");
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
    if (forcePrompt == null) {
      prompt = "";
      if (consolePresent) {
        if (changingLogicalPlan) {
          prompt = "logical-plan-change";
        } else {
          prompt = "apex";
        }
        if (currentApp != null) {
          prompt += " (";
          prompt += currentApp.getApplicationId().toString();
          prompt += ") ";
        }
        prompt += "> ";
      }
    } else {
      prompt = forcePrompt;
    }
    String line = reader.readLine(prompt, consolePresent ? null : (char)0);
    if (line == null) {
      return null;
    }
    return ltrim(line);
  }

  private List<ApplicationReport> getApplicationList()
  {
    try {
      return StramUtils.getApexApplicationList(yarnClient);
    } catch (Exception e) {
      throw new CliException("Error getting application list from resource manager", e);
    }
  }

  private String getContainerLongId(String containerId)
  {
    JSONObject json = getResource(StramWebServices.PATH_PHYSICAL_PLAN_CONTAINERS, currentApp);
    int shortId = 0;
    if (StringUtils.isNumeric(containerId)) {
      shortId = Integer.parseInt(containerId);
    }
    try {
      Object containersObj = json.get("containers");
      JSONArray containers;
      if (containersObj instanceof JSONArray) {
        containers = (JSONArray)containersObj;
      } else {
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
    } catch (JSONException ex) {
      // ignore
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
    } catch (YarnException rmExc) {
      throw new CliException("Unable to determine application status", rmExc);
    } catch (IOException rmExc) {
      throw new CliException("Unable to determine application status", rmExc);
    }
    return r;
  }

  private JSONObject getResource(String resourcePath, ApplicationReport appReport)
  {
    return getResource(new StramAgent.StramUriSpec().path(resourcePath), appReport, new WebServicesClient.GetWebServicesHandler<JSONObject>());
  }

  private JSONObject getResource(StramAgent.StramUriSpec uriSpec, ApplicationReport appReport)
  {
    return getResource(uriSpec, appReport, new WebServicesClient.GetWebServicesHandler<JSONObject>());
  }

  private JSONObject getResource(StramAgent.StramUriSpec uriSpec, ApplicationReport appReport, WebServicesClient.WebServicesHandler handler)
  {

    if (appReport == null) {
      throw new CliException("No application selected");
    }

    if (StringUtils.isEmpty(appReport.getTrackingUrl()) || appReport.getFinalApplicationStatus() != FinalApplicationStatus.UNDEFINED) {
      appReport = null;
      throw new CliException("Application terminated");
    }

    WebServicesClient wsClient = new WebServicesClient();
    try {
      return stramAgent.issueStramWebRequest(wsClient, appReport.getApplicationId().toString(), uriSpec, handler);
    } catch (Exception e) {
      // check the application status as above may have failed due application termination etc.
      if (appReport == currentApp) {
        currentApp = assertRunningApp(appReport);
      }
      throw new CliException("Failed to request web service for appid " + appReport.getApplicationId().toString(), e);
    }
  }

  private List<AppFactory> getMatchingAppFactories(StramAppLauncher submitApp, String matchString, boolean exactMatch)
  {
    try {
      List<AppFactory> cfgList = submitApp.getBundledTopologies();

      if (cfgList.isEmpty()) {
        return null;
      } else if (matchString == null) {
        return cfgList;
      } else {
        List<AppFactory> result = new ArrayList<>();
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
          } else if (appName.toLowerCase().contains(matchString) || (appAlias != null && appAlias.toLowerCase()
              .contains(matchString))) {
            result.add(ac);
          }
        }
        return result;
      }
    } catch (Exception ex) {
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
      } else {
        if (args[1].equals("help")) {
          printHelp("help", globalCommands.get("help"), os);
        } else {
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

  private class EchoCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      for (int i = 1; i < args.length; i++) {
        if (i > 1) {
          System.out.print(" ");
        }
        System.out.print(args[i]);
      }
      System.out.println();
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
      LOG.debug("Selected {} with tracking url {}", currentApp.getApplicationId(), currentApp.getTrackingUrl());
      getResource(StramWebServices.PATH_INFO, currentApp);
      if (consolePresent) {
        System.out.println("Connected to application " + currentApp.getApplicationId());
      }
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

      // see if the given config file is a config package
      ConfigPackage cp = null;
      String requiredAppPackageName = null;
      try {
        cp = new ConfigPackage(new File(commandLineInfo.configFile));
        requiredAppPackageName = cp.getAppPackageName();
      } catch (Exception ex) {
        // fall through, it's not a config package
      }
      try {
        Configuration config;
        String configFile = cp == null ? commandLineInfo.configFile : null;
        try {
          config = StramAppLauncher.getOverriddenConfig(StramClientUtils.addDTSiteResources(new Configuration()), configFile, commandLineInfo.overrideProperties);
          if (commandLineInfo.libjars != null) {
            commandLineInfo.libjars = expandCommaSeparatedFiles(commandLineInfo.libjars);
            if (commandLineInfo.libjars != null) {
              config.set(StramAppLauncher.LIBJARS_CONF_KEY_NAME, commandLineInfo.libjars);
            }
          }
          if (commandLineInfo.files != null) {
            commandLineInfo.files = expandCommaSeparatedFiles(commandLineInfo.files);
            if (commandLineInfo.files != null) {
              config.set(StramAppLauncher.FILES_CONF_KEY_NAME, commandLineInfo.files);
            }
          }
          if (commandLineInfo.archives != null) {
            commandLineInfo.archives = expandCommaSeparatedFiles(commandLineInfo.archives);
            if (commandLineInfo.archives != null) {
              config.set(StramAppLauncher.ARCHIVES_CONF_KEY_NAME, commandLineInfo.archives);
            }
          }
          if (commandLineInfo.origAppId != null) {
            config.set(StramAppLauncher.ORIGINAL_APP_ID, commandLineInfo.origAppId);
          }
          config.set(StramAppLauncher.QUEUE_NAME, commandLineInfo.queue != null ? commandLineInfo.queue : "default");
          if (commandLineInfo.tags != null) {
            config.set(StramAppLauncher.TAGS, commandLineInfo.tags);
          }
        } catch (Exception ex) {
          throw new CliException("Error opening the config XML file: " + configFile, ex);
        }
        StramAppLauncher submitApp;
        AppFactory appFactory = null;
        String matchString = null;
        if (commandLineInfo.args.length == 0) {
          if (commandLineInfo.origAppId == null) {
            throw new CliException("Launch requires an APA or JAR file when not resuming a terminated application");
          }
          submitApp = new StramAppLauncher(fs, config);
          appFactory = submitApp.new RecoveryAppFactory();
        } else {
          String fileName = expandFileName(commandLineInfo.args[0], true);
          if (commandLineInfo.args.length >= 2) {
            matchString = commandLineInfo.args[1];
          }
          if (fileName.endsWith(".json")) {
            File file = new File(fileName);
            submitApp = new StramAppLauncher(file.getName(), config);
            appFactory = new StramAppLauncher.JsonFileAppFactory(file);
            if (matchString != null) {
              LOG.warn("Match string \"{}\" is ignored for launching applications specified in JSON", matchString);
            }
          } else if (fileName.endsWith(".properties")) {
            File file = new File(fileName);
            submitApp = new StramAppLauncher(file.getName(), config);
            appFactory = new StramAppLauncher.PropertyFileAppFactory(file);
            if (matchString != null) {
              LOG.warn("Match string \"{}\" is ignored for launching applications specified in properties file", matchString);
            }
          } else {
            // see if it's an app package
            AppPackage ap = null;
            try {
              ap = newAppPackageInstance(new URI(fileName), true);
            } catch (Exception ex) {
              // It's not an app package
              if (requiredAppPackageName != null) {
                throw new CliException("Config package requires an app package name of \"" + requiredAppPackageName + "\"");
              }
            }

            if (ap != null) {
              try {
                if (!commandLineInfo.force) {
                  checkPlatformCompatible(ap);
                  checkConfigPackageCompatible(ap, cp);
                }
                launchAppPackage(ap, cp, commandLineInfo, reader);
                return;
              } finally {
                IOUtils.closeQuietly(ap);
              }
            }
            submitApp = getStramAppLauncher(fileName, config, commandLineInfo.ignorePom);
          }
        }
        submitApp.loadDependencies();

        if (commandLineInfo.origAppId != null) {
          // ensure app is not running
          ApplicationReport ar = null;
          try {
            ar = getApplication(commandLineInfo.origAppId);
          } catch (Exception e) {
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
              } else if (commandLineInfo.args[1].endsWith(".json")) {
                appFactory = new StramAppLauncher.JsonFileAppFactory(file);
              }
            }
          } catch (Exception | NoClassDefFoundError ex) {
            // ignore
          }
        }

        if (appFactory == null) {
          List<AppFactory> matchingAppFactories = getMatchingAppFactories(submitApp, matchString, commandLineInfo.exactMatch);
          if (matchingAppFactories == null || matchingAppFactories.isEmpty()) {
            throw new CliException("No applications matching \"" + matchString + "\" bundled in jar.");
          } else if (matchingAppFactories.size() == 1) {
            appFactory = matchingAppFactories.get(0);
          } else if (matchingAppFactories.size() > 1) {

            //Store the appNames sorted in alphabetical order and their position in matchingAppFactories list
            TreeMap<String, Integer> appNamesInAlphabeticalOrder = new TreeMap<>();
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
            HashMap<Integer, Integer> displayIndexToOriginalUnsortedIndexMap = new HashMap<>();
            for (Map.Entry<String, Integer> entry : appNamesInAlphabeticalOrder.entrySet()) {
              //Map display number of the app to original unsorted index
              displayIndexToOriginalUnsortedIndexMap.put(index, entry.getValue());

              //Display the app names
              System.out.printf("%3d. %s\n", index++, entry.getKey());
            }

            // Exit if not in interactive mode
            if (!consolePresent) {
              throw new CliException("More than one application in jar file match '" + matchString + "'");
            } else {

              boolean useHistory = reader.isHistoryEnabled();
              reader.setHistoryEnabled(false);
              History previousHistory = reader.getHistory();
              History dummyHistory = new MemoryHistory();
              reader.setHistory(dummyHistory);
              List<Completer> completers = new ArrayList<>(reader.getCompleters());
              for (Completer c : completers) {
                reader.removeCompleter(c);
              }
              reader.setHandleUserInterrupt(true);
              String optionLine;
              try {
                optionLine = reader.readLine("Choose application: ");
              } finally {
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
              } catch (Exception ex) {
                // ignore
              }
            }
          }

        }

        if (appFactory != null) {
          if (!commandLineInfo.localMode) {
            // see whether there is an app with the same name and user name running
            String appName = config.get(LogicalPlanConfiguration.KEY_APPLICATION_NAME, appFactory.getName());
            ApplicationReport duplicateApp = StramClientUtils.getStartedAppInstanceByName(yarnClient, appName, UserGroupInformation.getLoginUser().getUserName(), null);
            if (duplicateApp != null) {
              throw new CliException("Application with the name \"" + duplicateApp.getName() + "\" already running under the current user \"" + duplicateApp.getUser() + "\". Please choose another name. You can change the name by setting " + LogicalPlanConfiguration.KEY_APPLICATION_NAME);
            }

            // This is for suppressing System.out printouts from applications so that the user of CLI will not be confused by those printouts
            PrintStream originalStream = suppressOutput();
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
              appId = submitApp.launchApp(appFactory);
              currentApp = yarnClient.getApplicationReport(appId);
            } finally {
              restoreOutput(originalStream);
            }
            if (appId != null) {
              printJson("{\"appId\": \"" + appId + "\"}");
            }
          } else {
            submitApp.runLocal(appFactory);
          }
        } else {
          System.err.println("No application specified.");
        }
        submitApp.resetContextClassLoader();
      } finally {
        IOUtils.closeQuietly(cp);
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
        Map<String, String> sortedMap = new TreeMap<>();
        for (Map.Entry<String, String> entry : conf) {
          sortedMap.put(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, String> entry : sortedMap.entrySet()) {
          os.println(entry.getKey() + "=" + entry.getValue());
        }
      } else {
        String value = conf.get(args[1]);
        if (value != null) {
          os.println(value);
        }
      }
      closeOutputPrintStream(os);
    }

  }

  private ApplicationReport findApplicationReportFromAppNameOrId(String appNameOrId)
  {
    ApplicationReport app = getApplication(appNameOrId);
    if (app == null) {
      app = getApplicationByName(appNameOrId);
    }
    return app;
  }

  private class ShutdownAppCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      Map<String, ApplicationReport> appIdReports = new LinkedHashMap<>();

      if (args.length == 1) {
        if (currentApp == null) {
          throw new CliException("No application selected");
        } else {
          appIdReports.put(currentApp.getApplicationId().toString(), currentApp);
        }
      } else {
        String[] appNamesOrIds = Arrays.copyOfRange(args, 1, args.length);

        for (String appNameOrId : appNamesOrIds) {
          ApplicationReport ap = findApplicationReportFromAppNameOrId(appNameOrId);
          appIdReports.put(appNameOrId, ap);
        }
      }

      for (Map.Entry<String, ApplicationReport> entry : appIdReports.entrySet()) {
        String appNameOrId = entry.getKey();
        ApplicationReport app = entry.getValue();

        shutdownApp(appNameOrId, app);
      }
    }

    private void shutdownApp(String appNameOrId, ApplicationReport app)
    {
      if (app == null) {
        String errMessage = "Failed to request shutdown for app %s: Application with id or name %s not found%n";
        System.err.printf(errMessage, appNameOrId, appNameOrId);
        return;
      }

      try {
        JSONObject response = sendShutdownRequest(app);
        if (consolePresent) {
          System.out.printf("Shutdown of app %s requested: %s%n", app.getApplicationId().toString(), response);
        }
      } catch (Exception e) {
        String errMessage = "Failed to request shutdown for app %s: %s%n";
        System.err.printf(errMessage, app.getApplicationId().toString(), e.getMessage());
      } finally {
        if (currentApp != null) {
          if (app.getApplicationId().equals(currentApp.getApplicationId())) {
            currentApp = null;
          }
        }
      }
    }
  }

  protected JSONObject sendShutdownRequest(ApplicationReport app)
  {
    StramAgent.StramUriSpec uriSpec = new StramAgent.StramUriSpec().path(StramWebServices.PATH_SHUTDOWN);

    WebServicesClient.WebServicesHandler<JSONObject> handler = new WebServicesClient.WebServicesHandler<JSONObject>()
    {
      @Override
      public JSONObject process(WebResource.Builder webResource, Class<JSONObject> clazz)
      {
        return webResource.accept(MediaType.APPLICATION_JSON).post(clazz, new JSONObject());
      }
    };

    return getResource(uriSpec, app, handler);
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
          JSONArray tags = new JSONArray();
          for (String tag : ar.getApplicationTags()) {
            tags.put(tag);
          }
          jsonObj.put("tags", tags);

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
            } else {
              @SuppressWarnings("unchecked")
              Iterator<String> keys = jsonObj.keys();
              while (keys.hasNext()) {
                if (jsonObj.get(keys.next()).toString().toLowerCase().contains(args[1].toLowerCase())) {
                  jsonArray.put(jsonObj);
                  break;
                }
              }
            }
          } else {
            jsonArray.put(jsonObj);
          }
        }
        printJson(jsonArray, "apps");
        if (consolePresent) {
          System.out.println(runningCnt + " active, total " + totalCnt + " applications.");
        }
      } catch (Exception ex) {
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
        } else {
          try {
            yarnClient.killApplication(currentApp.getApplicationId());
            currentApp = null;
          } catch (YarnException e) {
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
          app = findApplicationReportFromAppNameOrId(args[i]);
          if (app == null) {
            throw new CliException("Streaming application with id or name " + args[i] + " is not found.");
          }

          yarnClient.killApplication(app.getApplicationId());
          if (app == currentApp) {
            currentApp = null;
          }
        }
        if (consolePresent) {
          System.out.println("Kill app requested");
        }
      } catch (YarnException e) {
        throw new CliException("Failed to kill " + ((app == null || app.getApplicationId() == null) ? "unknown application" : app.getApplicationId()) + ". Aborting killing of any additional applications.", e);
      } catch (NumberFormatException nfe) {
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
        try {
          topLevelHistory.flush();
        } catch (IOException ex) {
          LOG.warn("Cannot flush command history");
        }
      }
      if (changingLogicalPlanHistory != null) {
        try {
          changingLogicalPlanHistory.flush();
        } catch (IOException ex) {
          LOG.warn("Cannot flush command history");
        }
      }
      yarnClient.stop();
      System.exit(0);
    }

  }

  private class ListContainersCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      JSONObject json = getResource(StramWebServices.PATH_PHYSICAL_PLAN_CONTAINERS, currentApp);
      if (args.length == 1) {
        printJson(json);
      } else {
        Object containersObj = json.get("containers");
        JSONArray containers;
        if (containersObj instanceof JSONArray) {
          containers = (JSONArray)containersObj;
        } else {
          containers = new JSONArray();
          containers.put(containersObj);
        }
        if (containersObj == null) {
          System.out.println("No containers found!");
        } else {
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
      JSONObject json = getResource(StramWebServices.PATH_PHYSICAL_PLAN_OPERATORS, currentApp);

      if (args.length > 1) {
        String singleKey = "" + json.keys().next();
        JSONArray matches = new JSONArray();
        // filter operators
        JSONArray arr;
        Object obj = json.get(singleKey);
        if (obj instanceof JSONArray) {
          arr = (JSONArray)obj;
        } else {
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
          } else {
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
      try {
        printJson(getResource(StramWebServices.PATH_PHYSICAL_PLAN, currentApp));
      } catch (Exception e) {
        throw new CliException("Failed web service request for appid " + currentApp.getApplicationId().toString(), e);
      }
    }

  }

  private class KillContainerCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      for (int i = 1; i < args.length; i++) {
        String containerLongId = getContainerLongId(args[i]);
        if (containerLongId == null) {
          throw new CliException("Container " + args[i] + " not found");
        }
        try {
          StramAgent.StramUriSpec uriSpec = new StramAgent.StramUriSpec();
          uriSpec = uriSpec.path(StramWebServices.PATH_PHYSICAL_PLAN_CONTAINERS).path(URLEncoder.encode(containerLongId, "UTF-8")).path("kill");
          JSONObject response = getResource(uriSpec, currentApp, new WebServicesClient.WebServicesHandler<JSONObject>()
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
        } catch (Exception e) {
          throw new CliException("Failed web service request for appid " + currentApp.getApplicationId().toString(), e);
        }
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
          } catch (IOException e) {
            LOG.error("Error checking for input.", e);
          }
          return false;
        }

      };

      try {
        ClientRMHelper clientRMHelper = new ClientRMHelper(yarnClient, conf);
        boolean result = clientRMHelper.waitForCompletion(currentApp.getApplicationId(), cb, timeout * 1000);
        if (!result) {
          System.err.println("Application terminated unsuccessfully.");
        }
      } catch (YarnException e) {
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
      } else if (args.length <= 2) {
        String opId = args[1];
        List<RecordingInfo> recordingInfo = recordingsAgent.getRecordingInfo(currentApp.getApplicationId().toString(), opId);
        printJson(recordingInfo, "recordings");
      } else {
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
      StramAgent.StramUriSpec uriSpec = new StramAgent.StramUriSpec();
      uriSpec = uriSpec.path(StramWebServices.PATH_LOGICAL_PLAN).path("attributes");
      if (args.length > 1) {
        uriSpec = uriSpec.queryParam("attributeName", args[1]);
      }
      try {
        JSONObject response = getResource(uriSpec, currentApp);
        printJson(response);
      } catch (Exception e) {
        throw new CliException("Failed web service request for appid " + currentApp.getApplicationId().toString(), e);
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
      StramAgent.StramUriSpec uriSpec = new StramAgent.StramUriSpec();
      uriSpec = uriSpec.path(StramWebServices.PATH_LOGICAL_PLAN_OPERATORS).path(URLEncoder.encode(args[1], "UTF-8")).path("attributes");
      if (args.length > 2) {
        uriSpec = uriSpec.queryParam("attributeName", args[2]);
      }
      try {
        JSONObject response = getResource(uriSpec, currentApp);
        printJson(response);
      } catch (Exception e) {
        throw new CliException("Failed web service request for appid " + currentApp.getApplicationId().toString(), e);
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
      StramAgent.StramUriSpec uriSpec = new StramAgent.StramUriSpec();
      uriSpec = uriSpec.path(StramWebServices.PATH_LOGICAL_PLAN_OPERATORS).path(URLEncoder.encode(args[1], "UTF-8")).path("ports").path(URLEncoder.encode(args[2], "UTF-8")).path("attributes");
      if (args.length > 3) {
        uriSpec = uriSpec.queryParam("attributeName", args[3]);
      }
      try {
        JSONObject response = getResource(uriSpec, currentApp);
        printJson(response);
      } catch (Exception e) {
        throw new CliException("Failed web service request for appid " + currentApp.getApplicationId().toString(), e);
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
      StramAgent.StramUriSpec uriSpec = new StramAgent.StramUriSpec();
      uriSpec = uriSpec.path(StramWebServices.PATH_LOGICAL_PLAN_OPERATORS).path(URLEncoder.encode(args[1], "UTF-8")).path("properties");
      if (args.length > 2) {
        uriSpec = uriSpec.queryParam("propertyName", args[2]);
      }
      try {
        JSONObject response = getResource(uriSpec, currentApp);
        printJson(response);
      } catch (Exception e) {
        throw new CliException("Failed web service request for appid " + currentApp.getApplicationId().toString(), e);
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
      if (!NumberUtils.isDigits(args[1])) {
        throw new CliException("Operator ID must be a number");
      }
      String[] newArgs = new String[args.length - 1];
      System.arraycopy(args, 1, newArgs, 0, args.length - 1);
      PosixParser parser = new PosixParser();
      CommandLine line = parser.parse(GET_PHYSICAL_PROPERTY_OPTIONS.options, newArgs);
      String waitTime = line.getOptionValue(GET_PHYSICAL_PROPERTY_OPTIONS.waitTime.getOpt());
      String propertyName = line.getOptionValue(GET_PHYSICAL_PROPERTY_OPTIONS.propertyName.getOpt());
      StramAgent.StramUriSpec uriSpec = new StramAgent.StramUriSpec();
      uriSpec = uriSpec.path(StramWebServices.PATH_PHYSICAL_PLAN_OPERATORS).path(args[1]).path("properties");
      if (propertyName != null) {
        uriSpec = uriSpec.queryParam("propertyName", propertyName);
      }
      if (waitTime != null) {
        uriSpec = uriSpec.queryParam("waitTime", waitTime);
      }

      try {
        JSONObject response = getResource(uriSpec, currentApp);
        printJson(response);
      } catch (Exception e) {
        throw new CliException("Failed web service request for appid " + currentApp.getApplicationId().toString(), e);
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
      } else {
        StramAgent.StramUriSpec uriSpec = new StramAgent.StramUriSpec();
        uriSpec = uriSpec.path(StramWebServices.PATH_LOGICAL_PLAN_OPERATORS).path(URLEncoder.encode(args[1], "UTF-8")).path("properties");
        final JSONObject request = new JSONObject();
        request.put(args[2], args[3]);
        JSONObject response = getResource(uriSpec, currentApp, new WebServicesClient.WebServicesHandler<JSONObject>()
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
      if (!NumberUtils.isDigits(args[1])) {
        throw new CliException("Operator ID must be a number");
      }
      StramAgent.StramUriSpec uriSpec = new StramAgent.StramUriSpec();
      uriSpec = uriSpec.path(StramWebServices.PATH_PHYSICAL_PLAN_OPERATORS).path(args[1]).path("properties");
      final JSONObject request = new JSONObject();
      request.put(args[2], args[3]);
      JSONObject response = getResource(uriSpec, currentApp, new WebServicesClient.WebServicesHandler<JSONObject>()
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
        if (commandLineInfo.libjars != null) {
          config.set(StramAppLauncher.LIBJARS_CONF_KEY_NAME, commandLineInfo.libjars);
        }
      }

      if (commandLineInfo.args.length > 0) {
        // see if the first argument is actually an app package
        try (AppPackage ap = newAppPackageInstance(new URI(commandLineInfo.args[0]), false)) {
          new ShowLogicalPlanAppPackageCommand().execute(args, reader);
          return;
        } catch (Exception ex) {
          // fall through
        }

        String filename = expandFileName(commandLineInfo.args[0], true);
        if (commandLineInfo.args.length >= 2) {
          String appName = commandLineInfo.args[1];
          StramAppLauncher submitApp = getStramAppLauncher(filename, config, commandLineInfo.ignorePom);
          submitApp.loadDependencies();
          List<AppFactory> matchingAppFactories = getMatchingAppFactories(submitApp, appName, commandLineInfo.exactMatch);
          if (matchingAppFactories == null || matchingAppFactories.isEmpty()) {
            submitApp.resetContextClassLoader();
            throw new CliException("No application in jar file matches '" + appName + "'");
          } else if (matchingAppFactories.size() > 1) {
            submitApp.resetContextClassLoader();
            throw new CliException("More than one application in jar file match '" + appName + "'");
          } else {
            Map<String, Object> map = new HashMap<>();
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
              map.put("logicalPlan", LogicalPlanSerializer.convertToMap(logicalPlan, false));
            } finally {
              if (raw) {
                System.setOut(originalStream);
              }
            }
            printJson(map);
            submitApp.resetContextClassLoader();
          }
        } else {
          if (filename.endsWith(".json")) {
            File file = new File(filename);
            StramAppLauncher submitApp = new StramAppLauncher(file.getName(), config);
            AppFactory appFactory = new StramAppLauncher.JsonFileAppFactory(file);
            LogicalPlan logicalPlan = appFactory.createApp(submitApp.getLogicalPlanConfiguration());
            Map<String, Object> map = new HashMap<>();
            map.put("applicationName", appFactory.getName());
            map.put("logicalPlan", LogicalPlanSerializer.convertToMap(logicalPlan, false));
            printJson(map);
          } else if (filename.endsWith(".properties")) {
            File file = new File(filename);
            StramAppLauncher submitApp = new StramAppLauncher(file.getName(), config);
            AppFactory appFactory = new StramAppLauncher.PropertyFileAppFactory(file);
            LogicalPlan logicalPlan = appFactory.createApp(submitApp.getLogicalPlanConfiguration());
            Map<String, Object> map = new HashMap<>();
            map.put("applicationName", appFactory.getName());
            map.put("logicalPlan", LogicalPlanSerializer.convertToMap(logicalPlan, false));
            printJson(map);
          } else {
            StramAppLauncher submitApp = getStramAppLauncher(filename, config, commandLineInfo.ignorePom);
            submitApp.loadDependencies();
            List<Map<String, Object>> appList = new ArrayList<>();
            List<AppFactory> appFactoryList = submitApp.getBundledTopologies();
            for (AppFactory appFactory : appFactoryList) {
              Map<String, Object> m = new HashMap<>();
              m.put("name", appFactory.getName());
              appList.add(m);
            }
            printJson(appList, "applications");
            submitApp.resetContextClassLoader();
          }
        }
      } else {
        if (currentApp == null) {
          throw new CliException("No application selected");
        }
        JSONObject response = getResource(StramWebServices.PATH_LOGICAL_PLAN, currentApp);
        printJson(response);
      }
    }

  }

  private class ShowLogicalPlanAppPackageCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      try (AppPackage ap = newAppPackageInstance(new URI(args[1]), true)) {
        List<AppInfo> applications = ap.getApplications();

        if (args.length >= 3) {
          for (AppInfo appInfo : applications) {
            if (args[2].equals(appInfo.name)) {
              Map<String, Object> map = new HashMap<>();
              map.put("applicationName", appInfo.name);
              if (appInfo.dag != null) {
                map.put("logicalPlan", LogicalPlanSerializer.convertToMap(appInfo.dag, false));
              }
              if (appInfo.error != null) {
                map.put("error", appInfo.error);
              }
              printJson(map);
            }
          }
        } else {
          List<Map<String, Object>> appList = new ArrayList<>();
          for (AppInfo appInfo : applications) {
            Map<String, Object> m = new HashMap<>();
            m.put("name", appInfo.name);
            m.put("type", appInfo.type);
            appList.add(m);
          }
          printJson(appList, "applications");
        }
      }
    }

  }

  private File copyToLocal(String[] files) throws IOException
  {
    File tmpDir = new File(System.getProperty("java.io.tmpdir") + "/datatorrent/" + ManagementFactory.getRuntimeMXBean().getName());
    tmpDir.mkdirs();
    for (int i = 0; i < files.length; i++) {
      try {
        URI uri = new URI(files[i]);
        String scheme = uri.getScheme();
        if (scheme == null || scheme.equals("file")) {
          files[i] = uri.getPath();
        } else {
          try (FileSystem tmpFs = FileSystem.newInstance(uri, conf)) {
            Path srcPath = new Path(uri.getPath());
            Path dstPath = new Path(tmpDir.getAbsolutePath(), String.valueOf(i) + srcPath.getName());
            tmpFs.copyToLocalFile(srcPath, dstPath);
            files[i] = dstPath.toUri().getPath();
          }
        }
      } catch (URISyntaxException ex) {
        throw new RuntimeException(ex);
      }
    }

    return tmpDir;
  }

  private static Options GET_APP_PACKAGE_INFO_OPTIONS = new Options();

  static {
    GET_APP_PACKAGE_INFO_OPTIONS
        .addOption(new Option("withDescription", false, "Get default properties with description"));
  }

  public static class GetOperatorClassesCommandLineOptions
  {
    final Options options = new Options();
    final Option parent = add(new Option("parent", true, "Specify the parent class for the operators"));

    private Option add(Option opt)
    {
      this.options.addOption(opt);
      return opt;
    }

  }

  private static GetOperatorClassesCommandLineOptions GET_OPERATOR_CLASSES_OPTIONS = new GetOperatorClassesCommandLineOptions();

  static class GetAppPackageInfoCommandLineInfo
  {
    boolean provideDescription;
  }

  static GetAppPackageInfoCommandLineInfo getGetAppPackageInfoCommandLineInfo(String[] args) throws ParseException
  {
    CommandLineParser parser = new PosixParser();
    GetAppPackageInfoCommandLineInfo result = new GetAppPackageInfoCommandLineInfo();
    CommandLine line = parser.parse(GET_APP_PACKAGE_INFO_OPTIONS, args);
    result.provideDescription = line.hasOption("withDescription");
    return result;
  }

  static class GetOperatorClassesCommandLineInfo
  {
    String parent;
    String[] args;
  }

  static GetOperatorClassesCommandLineInfo getGetOperatorClassesCommandLineInfo(String[] args) throws ParseException
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
      String parentName = commandLineInfo.parent != null ? commandLineInfo.parent : GenericOperator.class.getName();
      String files = expandCommaSeparatedFiles(commandLineInfo.args[0]);
      if (files == null) {
        throw new CliException("File " + commandLineInfo.args[0] + " is not found");
      }
      String[] jarFiles = files.split(",");
      File tmpDir = copyToLocal(jarFiles);
      try {
        OperatorDiscoverer operatorDiscoverer = new OperatorDiscoverer(jarFiles);
        String searchTerm = commandLineInfo.args.length > 1 ? commandLineInfo.args[1] : null;
        Set<String> operatorClasses = operatorDiscoverer.getOperatorClasses(parentName, searchTerm);
        JSONObject json = new JSONObject();
        JSONArray arr = new JSONArray();
        JSONObject portClassHier = new JSONObject();
        JSONObject portTypesWithSchemaClasses = new JSONObject();

        JSONObject failed = new JSONObject();

        for (final String clazz : operatorClasses) {
          try {
            JSONObject oper = operatorDiscoverer.describeOperator(clazz);

            // add default value
            operatorDiscoverer.addDefaultValue(clazz, oper);

            // add class hierarchy info to portClassHier and fetch port types with schema classes
            operatorDiscoverer.buildAdditionalPortInfo(oper, portClassHier, portTypesWithSchemaClasses);

            Iterator portTypesIter = portTypesWithSchemaClasses.keys();
            while (portTypesIter.hasNext()) {
              if (!portTypesWithSchemaClasses.getBoolean((String)portTypesIter.next())) {
                portTypesIter.remove();
              }
            }

            arr.put(oper);
          } catch (Exception | NoClassDefFoundError ex) {
            // ignore this class
            final String cls = clazz;
            failed.put(cls, ex.toString());
          }
        }

        json.put("operatorClasses", arr);
        json.put("portClassHier", portClassHier);
        json.put("portTypesWithSchemaClasses", portTypesWithSchemaClasses);
        if (failed.length() > 0) {
          json.put("failedOperators", failed);
        }
        printJson(json);
      } finally {
        FileUtils.deleteDirectory(tmpDir);
      }
    }
  }

  private class GetJarOperatorPropertiesCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String files = expandCommaSeparatedFiles(args[1]);
      if (files == null) {
        throw new CliException("File " + args[1] + " is not found");
      }
      String[] jarFiles = files.split(",");
      File tmpDir = copyToLocal(jarFiles);
      try {
        OperatorDiscoverer operatorDiscoverer = new OperatorDiscoverer(jarFiles);
        Class<? extends Operator> operatorClass = operatorDiscoverer.getOperatorClass(args[2]);
        printJson(operatorDiscoverer.describeOperator(operatorClass.getName()));
      } finally {
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
          submitApp.resetContextClassLoader();
          throw new CliException("No application in jar file matches '" + appName + "'");
        } else if (matchingAppFactories.size() > 1) {
          submitApp.resetContextClassLoader();
          throw new CliException("More than one application in jar file match '" + appName + "'");
        } else {
          AppFactory appFactory = matchingAppFactories.get(0);
          LogicalPlan logicalPlan = appFactory.createApp(submitApp.getLogicalPlanConfiguration());
          File file = new File(outfilename);
          if (!file.exists()) {
            file.createNewFile();
          }
          LogicalPlanSerializer.convertToProperties(logicalPlan).save(file);
          submitApp.resetContextClassLoader();
        }
      } else {
        if (currentApp == null) {
          throw new CliException("No application selected");
        }
        JSONObject response = getResource(StramWebServices.PATH_LOGICAL_PLAN, currentApp);
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
      StramAgent.StramUriSpec uriSpec = new StramAgent.StramUriSpec();
      uriSpec = uriSpec.path(StramWebServices.PATH_LOGICAL_PLAN);
      try {
        final Map<String, Object> m = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();
        m.put("requests", logicalPlanRequestQueue);
        final JSONObject jsonRequest = new JSONObject(mapper.writeValueAsString(m));

        JSONObject response = getResource(uriSpec, currentApp, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource.Builder webResource, Class<JSONObject> clazz)
          {
            return webResource.accept(MediaType.APPLICATION_JSON).post(JSONObject.class, jsonRequest);
          }

        });
        printJson(response);
      } catch (Exception e) {
        throw new CliException("Failed web service request for appid " + currentApp.getApplicationId().toString(), e);
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
        List<String> commands = new ArrayList<>();
        while (true) {
          String line;
          if (consolePresent) {
            line = reader.readLine("macro def (" + name + ") > ");
          } else {
            line = reader.readLine("", (char)0);
          }
          if (line.equals("end")) {
            macros.put(name, commands);
            updateCompleter(reader);
            if (consolePresent) {
              System.out.println("Macro '" + name + "' created.");
            }
            return;
          } else if (line.equals("abort")) {
            System.err.println("Aborted");
            return;
          } else {
            commands.add(line);
          }
        }
      } catch (IOException ex) {
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
      } else if (args[1].equals("on")) {
        if (consolePresent) {
          pagerCommand = "less -F -X -r";
        }
      } else {
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
        if (appReport == null) {
          throw new CliException("Streaming application with id " + args[1] + " is not found.");
        }
      } else {
        if (currentApp == null) {
          throw new CliException("No application selected");
        }
        // refresh the state in currentApp
        currentApp = yarnClient.getApplicationReport(currentApp.getApplicationId());
        appReport = currentApp;
      }
      JSONObject response;
      try {
        response = getResource(StramWebServices.PATH_INFO, currentApp);
      } catch (Exception ex) {
        response = new JSONObject();
        response.put("startTime", appReport.getStartTime());
        response.put("id", appReport.getApplicationId().toString());
        response.put("name", appReport.getName());
        response.put("user", appReport.getUser());
      }
      response.put("state", appReport.getYarnApplicationState().name());
      response.put("trackingUrl", appReport.getTrackingUrl());
      response.put("finalStatus", appReport.getFinalApplicationStatus());
      JSONArray tags = new JSONArray();
      for (String tag : appReport.getApplicationTags()) {
        tags.put(tag);
      }
      response.put("tags", tags);
      printJson(response);
    }

  }

  private class GetContainerStackTrace implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String containerLongId = getContainerLongId(args[1]);
      if (containerLongId == null) {
        throw new CliException("Container " + args[1] + " not found");
      }

      JSONObject response;
      try {
        response = getResource(StramWebServices.PATH_PHYSICAL_PLAN_CONTAINERS + "/" + args[1] + "/" + StramWebServices.PATH_STACKTRACE, currentApp);
      } catch (Exception ex) {
        throw new CliException("Webservice call to AppMaster failed.", ex);
      }

      printJson(response);
    }

  }

  private class GetAppPackageInfoCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String[] tmpArgs = new String[args.length - 2];
      System.arraycopy(args, 2, tmpArgs, 0, args.length - 2);
      GetAppPackageInfoCommandLineInfo commandLineInfo = getGetAppPackageInfoCommandLineInfo(tmpArgs);
      try (AppPackage ap = newAppPackageInstance(new URI(args[1]), true)) {
        JSONSerializationProvider jomp = new JSONSerializationProvider();
        jomp.addSerializer(PropertyInfo.class,
            new AppPackage.PropertyInfoSerializer(commandLineInfo.provideDescription));
        JSONObject apInfo = new JSONObject(jomp.getContext(null).writeValueAsString(ap));
        apInfo.remove("name");
        printJson(apInfo);
      }
    }
  }

  private void checkConfigPackageCompatible(AppPackage ap, ConfigPackage cp)
  {
    if (cp == null) {
      return;
    }
    String requiredAppPackageName = cp.getAppPackageName();
    String requiredAppPackageGroupId = cp.getAppPackageGroupId();
    if (requiredAppPackageName != null && !requiredAppPackageName.equals(ap.getAppPackageName())) {
      throw new CliException("Config package requires an app package name of \"" + requiredAppPackageName + "\". The app package given has the name of \"" + ap.getAppPackageName() + "\"");
    }
    if (requiredAppPackageGroupId != null && !requiredAppPackageGroupId.equals(ap.getAppPackageGroupId())) {
      throw new CliException("Config package requires an app package group id of \"" + requiredAppPackageGroupId +
          "\". The app package given has the groupId of \"" + ap.getAppPackageGroupId() + "\"");
    }
    String requiredAppPackageMinVersion = cp.getAppPackageMinVersion();
    if (requiredAppPackageMinVersion != null && VersionInfo.compare(requiredAppPackageMinVersion, ap.getAppPackageVersion()) > 0) {
      throw new CliException("Config package requires an app package minimum version of \"" + requiredAppPackageMinVersion + "\". The app package given is of version \"" + ap.getAppPackageVersion() + "\"");
    }
    String requiredAppPackageMaxVersion = cp.getAppPackageMaxVersion();
    if (requiredAppPackageMaxVersion != null && VersionInfo.compare(requiredAppPackageMaxVersion, ap.getAppPackageVersion()) < 0) {
      throw new CliException("Config package requires an app package maximum version of \"" + requiredAppPackageMaxVersion + "\". The app package given is of version \"" + ap.getAppPackageVersion() + "\"");
    }
  }

  private void checkPlatformCompatible(AppPackage ap)
  {
    String apVersion = ap.getDtEngineVersion();
    VersionInfo actualVersion = VersionInfo.APEX_VERSION;
    if (!VersionInfo.isCompatible(actualVersion.getVersion(), apVersion)) {
      throw new CliException("This App Package is compiled with Apache Apex Core API version " + apVersion + ", which is incompatible with this Apex Core version " + actualVersion.getVersion());
    }
  }

  private void launchAppPackage(AppPackage ap, ConfigPackage cp, LaunchCommandLineInfo commandLineInfo, ConsoleReader reader) throws Exception
  {
    new LaunchCommand().execute(getLaunchAppPackageArgs(ap, cp, commandLineInfo, reader), reader);
  }

  String[] getLaunchAppPackageArgs(AppPackage ap, ConfigPackage cp, LaunchCommandLineInfo commandLineInfo, ConsoleReader reader) throws Exception
  {
    String matchAppName = null;
    if (commandLineInfo.args.length > 1) {
      matchAppName = commandLineInfo.args[1];
    }

    List<AppInfo> applications = new ArrayList<>(getAppsFromPackageAndConfig(ap, cp, commandLineInfo.useConfigApps));

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
    } else if (applications.size() == 1) {
      selectedApp = applications.get(0);
    } else {
      //Store the appNames sorted in alphabetical order and their position in matchingAppFactories list
      TreeMap<String, Integer> appNamesInAlphabeticalOrder = new TreeMap<>();
      // Display matching applications
      for (int i = 0; i < applications.size(); i++) {
        String appName = applications.get(i).name;
        appNamesInAlphabeticalOrder.put(appName, i);
      }

      //Create a mapping between the app display number and original index at matchingAppFactories
      int index = 1;
      HashMap<Integer, Integer> displayIndexToOriginalUnsortedIndexMap = new HashMap<>();
      for (Map.Entry<String, Integer> entry : appNamesInAlphabeticalOrder.entrySet()) {
        //Map display number of the app to original unsorted index
        displayIndexToOriginalUnsortedIndexMap.put(index, entry.getValue());

        //Display the app names
        System.out.printf("%3d. %s\n", index++, entry.getKey());
      }

      // Exit if not in interactive mode
      if (!consolePresent) {
        throw new CliException("More than one application in Application Package match '" + matchAppName + "'");
      } else {
        boolean useHistory = reader.isHistoryEnabled();
        reader.setHistoryEnabled(false);
        History previousHistory = reader.getHistory();
        History dummyHistory = new MemoryHistory();
        reader.setHistory(dummyHistory);
        List<Completer> completers = new ArrayList<>(reader.getCompleters());
        for (Completer c : completers) {
          reader.removeCompleter(c);
        }
        reader.setHandleUserInterrupt(true);
        String optionLine;
        try {
          optionLine = reader.readLine("Choose application: ");
        } finally {
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
        } catch (Exception ex) {
          // ignore
        }
      }
    }

    if (selectedApp == null) {
      throw new CliException("No application selected");
    }

    DTConfiguration launchProperties = getLaunchAppPackageProperties(ap, cp, commandLineInfo, selectedApp.name);
    String appFile = ap.tempDirectory() + "/app/" + selectedApp.file;

    List<String> launchArgs = new ArrayList<>();

    launchArgs.add("launch");
    launchArgs.add("-exactMatch");
    List<String> absClassPath = new ArrayList<>(ap.getClassPath());
    for (int i = 0; i < absClassPath.size(); i++) {
      String path = absClassPath.get(i);
      if (!path.startsWith("/")) {
        absClassPath.set(i, ap.tempDirectory() + "/" + path);
      }
    }

    if (cp != null) {
      StringBuilder files = new StringBuilder();
      for (String file : cp.getClassPath()) {
        if (files.length() != 0) {
          files.append(',');
        }
        files.append(cp.tempDirectory()).append(File.separatorChar).append(file);
      }
      if (!StringUtils.isBlank(files.toString())) {
        if (commandLineInfo.libjars != null) {
          commandLineInfo.libjars = files.toString() + "," + commandLineInfo.libjars;
        } else {
          commandLineInfo.libjars = files.toString();
        }
      }

      files.setLength(0);
      for (String file : cp.getFiles()) {
        if (files.length() != 0) {
          files.append(',');
        }
        files.append(cp.tempDirectory()).append(File.separatorChar).append(file);
      }
      if (!StringUtils.isBlank(files.toString())) {
        if (commandLineInfo.files != null) {
          commandLineInfo.files = files.toString() + "," + commandLineInfo.files;
        } else {
          commandLineInfo.files = files.toString();
        }
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
    if (commandLineInfo.archives != null) {
      launchArgs.add("-archives");
      launchArgs.add(commandLineInfo.archives);
    }
    if (commandLineInfo.files != null) {
      launchArgs.add("-files");
      launchArgs.add(commandLineInfo.files);
    }
    if (commandLineInfo.origAppId != null) {
      launchArgs.add("-originalAppId");
      launchArgs.add(commandLineInfo.origAppId);
    }
    if (commandLineInfo.queue != null) {
      launchArgs.add("-queue");
      launchArgs.add(commandLineInfo.queue);
    }
    if (commandLineInfo.tags != null) {
      launchArgs.add("-tags");
      launchArgs.add(commandLineInfo.tags);
    }
    launchArgs.add(appFile);
    if (!appFile.endsWith(".json") && !appFile.endsWith(".properties")) {
      launchArgs.add(selectedApp.name);
    }

    LOG.debug("Launch command: {}", StringUtils.join(launchArgs, " "));
    return launchArgs.toArray(new String[]{});
  }


  DTConfiguration getLaunchAppPackageProperties(AppPackage ap, ConfigPackage cp, LaunchCommandLineInfo commandLineInfo, String appName) throws Exception
  {
    DTConfiguration launchProperties = new DTConfiguration();

    List<AppInfo> applications = getAppsFromPackageAndConfig(ap, cp, commandLineInfo.useConfigApps);

    AppInfo selectedApp = null;
    for (AppInfo app : applications) {
      if (app.name.equals(appName)) {
        selectedApp = app;
        break;
      }
    }
    Map<String, PropertyInfo> defaultProperties = selectedApp == null ? ap.getDefaultProperties() : selectedApp.defaultProperties;
    Set<String> requiredProperties = new TreeSet<>(selectedApp == null ? ap.getRequiredProperties() : selectedApp.requiredProperties);

    for (Map.Entry<String, PropertyInfo> entry : defaultProperties.entrySet()) {
      launchProperties.set(entry.getKey(), entry.getValue().getValue(), Scope.TRANSIENT, entry.getValue().getDescription());
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
        String key = entry.getKey();
        if (key.startsWith(StreamingApplication.DT_PREFIX)
            || key.startsWith(StreamingApplication.APEX_PREFIX)) {
          launchProperties.set(key, entry.getValue(), Scope.TRANSIENT, null);
          requiredProperties.remove(key);
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
    if (cp != null) {
      Map<String, String> properties = cp.getProperties(appName);
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        launchProperties.set(entry.getKey(), entry.getValue(), Scope.TRANSIENT, null);
        requiredProperties.remove(entry.getKey());
      }
    } else if (commandLineInfo.configFile != null) {
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

    //StramClientUtils.evalProperties(launchProperties);
    return launchProperties;
  }

  private List<AppInfo> getAppsFromPackageAndConfig(AppPackage ap, ConfigPackage cp, String configApps)
  {
    if (cp == null || configApps == null || !(configApps.equals(CONFIG_INCLUSIVE) || configApps.equals(CONFIG_EXCLUSIVE))) {
      return ap.getApplications();
    }

    File src = new File(cp.tempDirectory(), "app");
    File dest = new File(ap.tempDirectory(), "app");

    if (!src.exists()) {
      return ap.getApplications();
    }

    if (configApps.equals(CONFIG_EXCLUSIVE)) {

      for (File file : dest.listFiles()) {

        if (file.getName().endsWith(".json")) {
          FileUtils.deleteQuietly(new File(dest, file.getName()));
        }
      }
    } else {
      for (File file : src.listFiles()) {
        FileUtils.deleteQuietly(new File(dest, file.getName()));
      }
    }

    for (File file : src.listFiles()) {
      try {
        FileUtils.moveFileToDirectory(file, dest, true);
      } catch (IOException e) {
        LOG.warn("Application from the config file {} failed while processing.", file.getName());
      }
    }

    try {
      FileUtils.deleteDirectory(src);
    } catch (IOException e) {
      LOG.warn("Failed to delete the Config Apps folder");
    }

    ap.processAppDirectory(configApps.equals(CONFIG_EXCLUSIVE));

    return ap.getApplications();
  }

  private class GetAppPackageOperatorsCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String[] tmpArgs = new String[args.length - 1];
      System.arraycopy(args, 1, tmpArgs, 0, args.length - 1);
      GetOperatorClassesCommandLineInfo commandLineInfo = getGetOperatorClassesCommandLineInfo(tmpArgs);
      try (AppPackage ap = newAppPackageInstance(new URI(commandLineInfo.args[0]), true)) {
        List<String> newArgs = new ArrayList<>();
        List<String> jars = new ArrayList<>();
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
    }

  }

  private class GetAppPackageOperatorPropertiesCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      try (AppPackage ap = newAppPackageInstance(new URI(args[1]), true)) {
        List<String> newArgs = new ArrayList<>();
        List<String> jars = new ArrayList<>();
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
    }

  }

  private enum AttributesType
  {
    APPLICATION, OPERATOR, PORT
  }

  private class ListDefaultAttributesCommand implements Command
  {
    private final AttributesType type;

    protected ListDefaultAttributesCommand(@NotNull AttributesType type)
    {
      this.type = Preconditions.checkNotNull(type);
    }

    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      JSONObject result;
      if (type == AttributesType.APPLICATION) {
        result = TypeDiscoverer.getAppAttributes();
      } else if (type == AttributesType.OPERATOR) {
        result = TypeDiscoverer.getOperatorAttributes();
      } else {
        //get port attributes
        result = TypeDiscoverer.getPortAttributes();
      }
      printJson(result);
    }
  }

  private class CleanAppDirectoriesCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      JSONObject result = new JSONObject();
      JSONArray appArray = new JSONArray();
      List<ApplicationReport> apps = StramClientUtils.cleanAppDirectories(yarnClient, conf, fs,
          System.currentTimeMillis() - Long.valueOf(args[1]));
      for (ApplicationReport app : apps) {
        appArray.put(app.getApplicationId().toString());
      }
      result.put("applications", appArray);
      printJson(result);
    }
  }

  private class SetLogLevelCommand implements Command
  {

    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      ApplicationReport appReport = currentApp;
      String target = args[1];
      String logLevel = args[2];

      StramAgent.StramUriSpec uriSpec = new StramAgent.StramUriSpec();
      uriSpec = uriSpec.path(StramWebServices.PATH_LOGGERS);
      final JSONObject request = buildRequest(target, logLevel);

      JSONObject response = getResource(uriSpec, appReport, new WebServicesClient.WebServicesHandler<JSONObject>()
      {
        @Override
        public JSONObject process(WebResource.Builder webResource, Class<JSONObject> clazz)
        {
          return webResource.accept(MediaType.APPLICATION_JSON).post(JSONObject.class, request);
        }

      });

      printJson(response);
    }

    private JSONObject buildRequest(String target, String logLevel) throws JSONException
    {
      JSONObject request = new JSONObject();
      JSONArray loggers = new JSONArray();
      JSONObject targetAndLevelPair = new JSONObject();

      targetAndLevelPair.put("target", target);
      targetAndLevelPair.put("logLevel", logLevel);

      loggers.put(targetAndLevelPair);

      request.put("loggers", loggers);

      return request;
    }
  }

  @SuppressWarnings("static-access")
  public static class GetPhysicalPropertiesCommandLineOptions
  {
    final Options options = new Options();
    final Option propertyName = add(OptionBuilder.withArgName("property name").hasArg().withDescription("The name of the property whose value needs to be retrieved").create("propertyName"));
    final Option waitTime = add(OptionBuilder.withArgName("wait time").hasArg().withDescription("How long to wait to get the result").create("waitTime"));

    private Option add(Option opt)
    {
      this.options.addOption(opt);
      return opt;
    }
  }

  private static GetPhysicalPropertiesCommandLineOptions GET_PHYSICAL_PROPERTY_OPTIONS = new GetPhysicalPropertiesCommandLineOptions();

  @SuppressWarnings("static-access")
  public static class LaunchCommandLineOptions
  {
    final Options options = new Options();
    final Option local = add(new Option("local", "Run application in local mode."));
    final Option configFile = add(OptionBuilder.withArgName("configuration file").hasArg().withDescription("Specify an application configuration file.").create("conf"));
    final Option apConfigFile = add(OptionBuilder.withArgName("app package configuration file").hasArg().withDescription("Specify an application configuration file within the app package if launching an app package.").create("apconf"));
    final Option defProperty = add(OptionBuilder.withArgName("property=value").hasArg().withDescription("Use value for given property.").create("D"));
    final Option libjars = add(OptionBuilder.withArgName("comma separated list of libjars").hasArg().withDescription("Specify comma separated jar files or other resource files to include in the classpath.").create("libjars"));
    final Option files = add(OptionBuilder.withArgName("comma separated list of files").hasArg().withDescription("Specify comma separated files to be copied on the compute machines.").create("files"));
    final Option archives = add(OptionBuilder.withArgName("comma separated list of archives").hasArg().withDescription("Specify comma separated archives to be unarchived on the compute machines.").create("archives"));
    final Option ignorePom = add(new Option("ignorepom", "Do not run maven to find the dependency"));
    final Option originalAppID = add(OptionBuilder.withArgName("application id").hasArg().withDescription("Specify original application identifier for restart.").create("originalAppId"));
    final Option exactMatch = add(new Option("exactMatch", "Only consider applications with exact app name"));
    final Option queue = add(OptionBuilder.withArgName("queue name").hasArg().withDescription("Specify the queue to launch the application").create("queue"));
    final Option tags = add(OptionBuilder.withArgName("comma separated tags").hasArg().withDescription("Specify the tags for the application").create("tags"));
    final Option force = add(new Option("force", "Force launch the application. Do not check for compatibility"));
    final Option useConfigApps = add(OptionBuilder.withArgName("inclusive or exclusive").hasArg().withDescription("\"inclusive\" - merge the apps in config and app package. \"exclusive\" - only show config package apps.").create("useConfigApps"));

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
      result.overrideProperties = new HashMap<>();
      for (String def : defs) {
        int equal = def.indexOf('=');
        if (equal < 0) {
          result.overrideProperties.put(def, null);
        } else {
          result.overrideProperties.put(def.substring(0, equal), def.substring(equal + 1));
        }
      }
    }
    result.libjars = line.getOptionValue(LAUNCH_OPTIONS.libjars.getOpt());
    result.archives = line.getOptionValue(LAUNCH_OPTIONS.archives.getOpt());
    result.files = line.getOptionValue(LAUNCH_OPTIONS.files.getOpt());
    result.queue = line.getOptionValue(LAUNCH_OPTIONS.queue.getOpt());
    result.tags = line.getOptionValue(LAUNCH_OPTIONS.tags.getOpt());
    result.args = line.getArgs();
    result.origAppId = line.getOptionValue(LAUNCH_OPTIONS.originalAppID.getOpt());
    result.exactMatch = line.hasOption("exactMatch");
    result.force = line.hasOption("force");
    result.useConfigApps = line.getOptionValue(LAUNCH_OPTIONS.useConfigApps.getOpt());

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
    String tags;
    String archives;
    String origAppId;
    boolean exactMatch;
    boolean force;
    String[] args;
    String useConfigApps;
  }

  @SuppressWarnings("static-access")
  public static Options getShowLogicalPlanCommandLineOptions()
  {
    Options options = new Options();
    Option libjars = OptionBuilder.withArgName("comma separated list of jars").hasArg().withDescription("Specify comma separated jar/resource files to include in the classpath.").create("libjars");
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

  public void mainHelper() throws Exception
  {
    init();
    run();
    System.exit(lastCommandError ? 1 : 0);
  }

  public static void main(final String[] args) throws Exception
  {
    LoggerUtil.setupMDC("client");
    final ApexCli shell = new ApexCli();
    shell.preImpersonationInit(args);
    String hadoopUserName = System.getenv("HADOOP_USER_NAME");
    if (UserGroupInformation.isSecurityEnabled()
        && StringUtils.isNotBlank(hadoopUserName)
        && !hadoopUserName.equals(UserGroupInformation.getLoginUser().getUserName())) {
      LOG.info("You ({}) are running as user {}", UserGroupInformation.getLoginUser().getUserName(), hadoopUserName);
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(hadoopUserName, UserGroupInformation.getLoginUser());
      ugi.doAs(new PrivilegedExceptionAction<Void>()
      {
        @Override
        public Void run() throws Exception
        {
          shell.mainHelper();
          return null;
        }
      });
    } else {
      shell.mainHelper();
    }
  }

}
