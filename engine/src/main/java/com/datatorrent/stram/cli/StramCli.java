/**
 * Copyright (c) 2012-2013 DataTorrent, Inc. All rights reserved.
 */
package com.datatorrent.stram.cli;

import com.datatorrent.stram.codec.LogicalPlanSerializer;
import com.datatorrent.stram.cli.StramAppLauncher.AppConfig;
import com.datatorrent.stram.cli.StramClientUtils.ClientRMHelper;
import com.datatorrent.stram.cli.StramClientUtils.YarnClientHelper;
import com.datatorrent.stram.plan.logical.*;
import com.datatorrent.stram.security.StramUserLogin;
import com.datatorrent.stram.util.VersionInfo;
import com.datatorrent.stram.util.WebServicesClient;
import com.datatorrent.stram.webapp.StramWebServices;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.DAGPropertiesBuilder;
import com.datatorrent.stram.cli.StramAppLauncher.CommandLineInfo;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import javax.ws.rs.core.MediaType;

import jline.console.completer.*;
import jline.console.ConsoleReader;
import jline.console.history.History;
import jline.console.history.FileHistory;
import jline.console.history.MemoryHistory;

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
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.tools.ant.DirectoryScanner;
import org.codehaus.jettison.json.JSONException;

/**
 *
 * Provides command line interface for a streaming application on hadoop (yarn)<p>
 *
 * @since 0.3.2
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
  private final List<LogicalPlanRequest> logicalPlanRequestQueue = new ArrayList<LogicalPlanRequest>();
  private FileHistory topLevelHistory;
  private FileHistory changingLogicalPlanHistory;
  private boolean licensedVersion = true;

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

  public static class Tokenizer
  {
    private static void appendToCommandBuffer(List<String> commandBuffer, StringBuffer buf, boolean potentialEmptyArg)
    {
      if (potentialEmptyArg || buf.length() > 0) {
        commandBuffer.add(buf.toString());
        buf.setLength(0);
      }
    }

    private static List<String> startNewCommand(List<List<String>> resultBuffer)
    {
      List<String> newCommand = new ArrayList<String>();
      resultBuffer.add(newCommand);
      return newCommand;
    }

    public static List<String[]> tokenize(String commandLine)
    {
      List<List<String>> resultBuffer = new ArrayList<List<String>>();
      List<String> commandBuffer = startNewCommand(resultBuffer);

      if (commandLine != null) {
        commandLine = ltrim(commandLine);
        if (commandLine.startsWith("#")) {
          return null;
        }

        int len = commandLine.length();
        boolean insideQuotes = false;
        boolean potentialEmptyArg = false;
        StringBuffer buf = new StringBuffer();

        for (int i = 0; i < len; ++i) {
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
              if (c == ';') {
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

  private abstract class LicensedCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      if (licensedVersion) {
        executeLicensed(args, reader);
      }
      else {
        System.out.println("This command is only valid in the licensed version of DataTorrent. Visit http://datatorrent.com for information of obtaining a licensed version.");
      }
    }

    public abstract void executeLicensed(String[] args, ConsoleReader reader) throws Exception;

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
        throw new CliException("Parameter error");
      }
    }

    void printUsage(String cmd)
    {
      System.err.print("Usage: " + cmd);
      if (requiredArgs != null) {
        for (String arg : requiredArgs) {
          System.err.print(" <" + arg + ">");
        }
      }
      if (optionalArgs != null) {
        for (String arg : optionalArgs) {
          System.err.print(" [<" + arg + ">]");
        }
      }
      System.err.println();
    }

  }

  private static class OptionsCommandSpec extends CommandSpec
  {
    Options options;

    OptionsCommandSpec(Command command, String[] requiredArgs, String[] optionalArgs, String description, Options options)
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

  StramCli()
  {
    globalCommands.put("help", new CommandSpec(new HelpCommand(), null, null, "Show help"));
    globalCommands.put("connect", new CommandSpec(new ConnectCommand(), new String[] {"app-id"}, null, "Connect to an app"));
    globalCommands.put("launch", new OptionsCommandSpec(new LaunchCommand(), new String[] {"jar-file"}, new String[] {"class-name/property-file"}, "Launch an app", StramAppLauncher.getCommandLineOptions()));
    /* BEGIN to be deleted */
    globalCommands.put("launch-local", new OptionsCommandSpec(new LaunchCommand(), new String[] {"jar-file"}, new String[] {"class-name/property-file"}, "Launch an app", StramAppLauncher.getCommandLineOptions()));
    /* END to be deleted */
    globalCommands.put("shutdown-app", new CommandSpec(new ShutdownAppCommand(), new String[] {"app-id"}, null, "Shutdown an app"));
    globalCommands.put("list-apps", new CommandSpec(new ListAppsCommand(), null, new String[] {"pattern"}, "List applications"));
    globalCommands.put("kill-app", new CommandSpec(new KillAppCommand(), new String[] {"app-id"}, null, "Kill an app"));
    globalCommands.put("show-logical-plan", new CommandSpec(new ShowLogicalPlanCommand(), new String[] {"jar-file", "class-name"}, null, "Show logical plan of an app class"));
    globalCommands.put("alias", new CommandSpec(new AliasCommand(), new String[] {"alias-name", "command"}, null, "Create a command alias"));
    globalCommands.put("source", new CommandSpec(new SourceCommand(), new String[] {"file"}, null, "Execute the commands in a file"));
    globalCommands.put("exit", new CommandSpec(new ExitCommand(), null, null, "Exit the CLI"));
    globalCommands.put("begin-macro", new CommandSpec(new BeginMacroCommand(), new String[] {"name"}, null, "Begin Macro Definition ($1...$9 to access parameters and type 'end' to end the definition)"));
    globalCommands.put("dump-properties-file", new CommandSpec(new DumpPropertiesFileCommand(), new String[] {"out-file", "jar-file", "class-name"}, null, "Dump the properties file of an app class"));
    globalCommands.put("get-app-info", new CommandSpec(new GetAppInfoCommand(), new String[] {"app-id"}, null, "Get the information of an app"));

    connectedCommands.put("list-containers", new CommandSpec(new ListContainersCommand(), null, null, "List containers"));
    connectedCommands.put("list-operators", new CommandSpec(new ListOperatorsCommand(), null, new String[] {"pattern"}, "List operators"));
    connectedCommands.put("kill-container", new CommandSpec(new KillContainerCommand(), new String[] {"container-id"}, null, "Kill a container"));
    connectedCommands.put("shutdown-app", new CommandSpec(new ShutdownAppCommand(), null, null, "Shutdown an app"));
    connectedCommands.put("kill-app", new CommandSpec(new KillAppCommand(), null, new String[] {"app-id"}, "Kill an app"));
    connectedCommands.put("wait", new CommandSpec(new WaitCommand(), new String[] {"timeout"}, null, "Wait for completion of current application"));
    connectedCommands.put("start-recording", new CommandSpec(new StartRecordingCommand(), new String[] {"operator-id"}, new String[] {"port-name"}, "Start recording"));
    connectedCommands.put("stop-recording", new CommandSpec(new StopRecordingCommand(), new String[] {"operator-id"}, new String[] {"port-name"}, "Stop recording"));
    connectedCommands.put("sync-recording", new CommandSpec(new SyncRecordingCommand(), new String[] {"operator-id"}, new String[] {"port-name"}, "Sync recording"));
    connectedCommands.put("get-operator-attributes", new CommandSpec(new GetOperatorAttributesCommand(), new String[] {"operator-name"}, new String[] {"attribute-name"}, "Get attributes of an operator"));
    connectedCommands.put("get-operator-properties", new CommandSpec(new GetOperatorPropertiesCommand(), new String[] {"operator-name"}, new String[] {"property-name"}, "Get properties of an operator"));
    connectedCommands.put("set-operator-property", new CommandSpec(new SetOperatorPropertyCommand(), new String[] {"operator-name", "property-name", "property-value"}, null, "Set a property of an operator"));
    connectedCommands.put("get-app-attributes", new CommandSpec(new GetAppAttributesCommand(), null, new String[] {"attribute-name"}, "Get attributes of the connected app"));
    connectedCommands.put("get-port-attributes", new CommandSpec(new GetPortAttributesCommand(), new String[] {"operator-name", "port-name"}, new String[] {"attribute-name"}, "Get attributes of a port"));
    connectedCommands.put("begin-logical-plan-change", new CommandSpec(new BeginLogicalPlanChangeCommand(), null, null, "Begin Logical Plan Change"));
    connectedCommands.put("show-logical-plan", new CommandSpec(new ShowLogicalPlanCommand(), null, new String[] {"jar-file", "class-name"}, "Show logical plan of an app class"));
    connectedCommands.put("dump-properties-file", new CommandSpec(new DumpPropertiesFileCommand(), new String[] {"out-file"}, new String[] {"jar-file", "class-name"}, "Dump the properties file of an app class"));
    connectedCommands.put("get-app-info", new CommandSpec(new GetAppInfoCommand(), null, new String[] {"app-id"}, "Get the information of an app"));
    connectedCommands.put("create-alert", new CommandSpec(new CreateAlertCommand(), new String[] {"name", "file"}, null, "Create an alert with the name and the given file that contains the spec"));
    connectedCommands.put("delete-alert", new CommandSpec(new DeleteAlertCommand(), new String[] {"name"}, null, "Delete an alert with the given name"));
    connectedCommands.put("list-alerts", new CommandSpec(new ListAlertsCommand(), null, null, "List all alerts"));

    logicalPlanChangeCommands.put("help", new CommandSpec(new HelpCommand(), null, null, "Show help"));
    logicalPlanChangeCommands.put("create-operator", new CommandSpec(new CreateOperatorCommand(), new String[] {"operator-name", "class-name"}, null, "Create an operator"));
    logicalPlanChangeCommands.put("create-stream", new CommandSpec(new CreateStreamCommand(), new String[] {"stream-name", "from-operator-name", "from-port-name", "to-operator-name", "to-port-name"}, null, "Create a stream"));
    logicalPlanChangeCommands.put("add-stream-sink", new CommandSpec(new AddStreamSinkCommand(), new String[] {"stream-name", "to-operator-name", "to-port-name"}, null, "Add a sink to an existing stream"));
    logicalPlanChangeCommands.put("remove-operator", new CommandSpec(new RemoveOperatorCommand(), new String[] {"operator-name"}, null, "Remove an operator"));
    logicalPlanChangeCommands.put("remove-stream", new CommandSpec(new RemoveStreamCommand(), new String[] {"stream-name"}, null, "Remove a stream"));
    logicalPlanChangeCommands.put("set-operator-property", new CommandSpec(new SetOperatorPropertyCommand(), new String[] {"operator-name", "property-name", "property-value"}, null, "Set a property of an operator"));
    logicalPlanChangeCommands.put("set-operator-attribute", new CommandSpec(new SetOperatorAttributeCommand(), new String[] {"operator-name", "attr-name", "attr-value"}, null, "Set an attribute of an operator"));
    logicalPlanChangeCommands.put("set-port-attribute", new CommandSpec(new SetPortAttributeCommand(), new String[] {"operator-name", "port-name", "attr-name", "attr-value"}, null, "Set an attribute of a port"));
    logicalPlanChangeCommands.put("set-stream-attribute", new CommandSpec(new SetStreamAttributeCommand(), new String[] {"stream-name", "attr-name", "attr-value"}, null, "Set an attribute of a stream"));
    logicalPlanChangeCommands.put("show-queue", new CommandSpec(new ShowQueueCommand(), null, null, "Show the queue of the plan change"));
    logicalPlanChangeCommands.put("submit", new CommandSpec(new SubmitCommand(), null, null, "Submit the plan change"));
    logicalPlanChangeCommands.put("abort", new CommandSpec(new AbortCommand(), null, null, "Abort the plan change"));
    StramClientUtils.addStramResources(conf);

  }

  private static String expandFileName(String fileName, boolean expandWildCard) throws IOException
  {
    // TODO: need to work with other users
    if (fileName.startsWith("~" + File.separator)) {
      fileName = System.getProperty("user.home") + fileName.substring(1);
    }
    fileName = new File(fileName).getCanonicalPath();
    LOG.debug("Canonical path: {}", fileName);
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

    try {
      com.datatorrent.stram.StramAppMaster.class.getClass();
    }
    catch (NoClassDefFoundError ex) {
      System.out.println();
      System.out.println("Warning: This version of DataTorrent is free and only valid in local mode.");
      System.out.println("For information of how to obtain a licensed version to be run in a cluster, please visit http://datatorrent.com");
      System.out.println();
      licensedVersion = false;
    }
  }

  private void processSourceFile(String fileName, ConsoleReader reader) throws FileNotFoundException, IOException
  {
    boolean consolePresentSaved = consolePresent;
    consolePresent = false;
    try {
      FileLineReader fr = new FileLineReader(fileName);
      String line;
      while ((line = fr.readLine("")) != null) {
        processLine(line, fr, true);
      }
      fr.close();
    }
    finally {
      consolePresent = consolePresentSaved;
    }
  }

  private List<Completer> defaultCompleters()
  {
    List<Completer> completers = new LinkedList<Completer>();
    completers.add(new StringsCompleter(connectedCommands.keySet().toArray(new String[] {})));
    completers.add(new StringsCompleter(globalCommands.keySet().toArray(new String[] {})));
    completers.add(new StringsCompleter(logicalPlanChangeCommands.keySet().toArray(new String[] {})));
    completers.add(new StringsCompleter(aliases.keySet().toArray(new String[] {})));
    completers.add(new StringsCompleter(macros.keySet().toArray(new String[] {})));

    List<Completer> launchCompleters = new LinkedList<Completer>();
    launchCompleters.add(new StringsCompleter(new String[] {"launch", "launch-local", "show-logical-plan", "dump-properties-file", "source", "create-alert"}));
    launchCompleters.add(new FileNameCompleter()); // jarFile
    launchCompleters.add(new FileNameCompleter()); // topology
    completers.add(new ArgumentCompleter(launchCompleters));
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

  public void run() throws IOException
  {
    ConsoleReader reader = new ConsoleReader();
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
    if (topLevelHistory != null && changingLogicalPlanHistory != null) {
      topLevelHistory.flush();
      changingLogicalPlanHistory.flush();
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
        // Search for $0..$9 within the each line and replace by corresponding args
        int currentIndex = line.indexOf('$', previousIndex);
        if (currentIndex > 0 && line.length() > currentIndex + 1) {
          int argIndex = line.charAt(currentIndex + 1) - '0';
          if (args.length > argIndex && argIndex >= 0) {
            // Replace $0 with macro name or $1..$9 with input arguments
            expandedLine += line.substring(previousIndex, currentIndex);
            expandedLine += args[argIndex];
          }
          else if (argIndex >= 0 && argIndex <= 9) {
            // Arguments for $1..$9 were not supplied - replace with empty strings
            expandedLine += line.substring(previousIndex, currentIndex);
          }
          else {
            // Outside valid arguments range - ignore and do not replace
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
      //LOG.debug("line: \"{}\"", line);
      List<String[]> commands = Tokenizer.tokenize(line);
      if (commands == null) {
        return;
      }
      for (String[] args : commands) {
        if (args.length == 0 || StringUtils.isBlank(args[0])) {
          continue;
        }
        //ObjectMapper mapper = new ObjectMapper();
        //LOG.debug("Got: {}", mapper.writeValueAsString(args));
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
        }
      }
    }
    catch (CliException e) {
      System.err.println(e.getMessage());
      LOG.debug("Error processing line: " + line, e);
    }
    catch (Exception e) {
      System.err.println("Unexpected error: " + e);
      LOG.error("Error processing line: {}", line, e);
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
      if (cs instanceof OptionsCommandSpec) {
        OptionsCommandSpec ocs = (OptionsCommandSpec)cs;
        if (ocs.options != null) {
          System.out.print(" [options]");
        }
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
      if (cs instanceof OptionsCommandSpec) {
        OptionsCommandSpec ocs = (OptionsCommandSpec)cs;
        if (ocs.options != null) {
          System.out.println("\tOptions:");
          HelpFormatter formatter = new HelpFormatter();
          PrintWriter pw = new PrintWriter(System.out);
          formatter.printOptions(pw, 80, ocs.options, 12, 4);
          pw.flush();
        }
      }
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
    return ltrim(line);
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

  private String getContainerLongId(String containerId)
  {
    ClientResponse rsp = getResource(StramWebServices.PATH_CONTAINERS, currentApp);
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
    catch (YarnRemoteException rmExc) {
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
    WebResource r = client.resource("http://" + appReport.getTrackingUrl()).path(StramWebServices.PATH).path(resourcePath);
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

  private WebResource getPostResource(WebServicesClient webServicesClient, ApplicationReport appReport)
  {
    if (appReport == null) {
      throw new CliException("No application selected");
    }
    // YARN-156 WebAppProxyServlet does not support POST - for now bypass it for this request
    appReport = assertRunningApp(appReport); // or else "N/A" might be there..
    String trackingUrl = appReport.getOriginalTrackingUrl();

    Client wsClient = webServicesClient.getClient();
    wsClient.setFollowRedirects(true);
    return wsClient.resource("http://" + trackingUrl).path(StramWebServices.PATH);
  }

  private List<AppConfig> getMatchingAppConfigs(StramAppLauncher submitApp, String matchString)
  {
    try {
      List<AppConfig> cfgList = submitApp.getBundledTopologies();

      if (cfgList.isEmpty()) {
        return null;
      }
      else if (matchString == null) {
        return cfgList;
      }
      else {
        List<AppConfig> result = new ArrayList<AppConfig>();
        for (AppConfig ac : cfgList) {
          if (ac.getName().matches(".*" + matchString + ".*")) {
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
      System.out.println("GLOBAL COMMANDS EXCEPT WHEN CHANGING LOGICAL PLAN:\n");
      printHelp(globalCommands);
      System.out.println();
      System.out.println("COMMANDS WHEN CONNECTED TO AN APP (via connect <appid>) EXCEPT WHEN CHANGING LOGICAL PLAN:\n");
      printHelp(connectedCommands);
      System.out.println();
      System.out.println("COMMANDS WHEN CHANGING LOGICAL PLAN (via begin-logical-plan-change):\n");
      printHelp(logicalPlanChangeCommands);
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
        throw new CliException("Invalid application id: " + args[1]);
      }

      boolean connected = false;
      try {
        LOG.debug("Selected {} with tracking url {}", currentApp.getApplicationId(), currentApp.getTrackingUrl());
        ClientResponse rsp = getResource(StramWebServices.PATH_INFO, currentApp);
        rsp.getEntity(JSONObject.class);
        System.out.println("Connected to application " + currentApp.getApplicationId() + ".");
        connected = true; // set as current only upon successful connection
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

  private class LaunchCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      String[] newArgs = new String[args.length - 1];
      System.arraycopy(args, 1, newArgs, 0, args.length - 1);
      CommandLineInfo commandLineInfo = StramAppLauncher.getCommandLineInfo(newArgs);
      /* BEGIN to be deleted */
      if (args[0].equals("launch-local")) {
        commandLineInfo.localMode = true;
      }
      /* END to be deleted */

      if (!commandLineInfo.localMode && !licensedVersion) {
        System.out.println("This free version only supports launching in local mode. Use the command 'launch-local' to launch in local mode.");
        System.out.println("Visit http://datatorrent.com for information on obtaining a licensed version of this software.");
        return;
      }
      Configuration config = StramAppLauncher.getConfig(commandLineInfo.configFile, commandLineInfo.overrideProperties);
      String fileName = expandFileName(commandLineInfo.args[0], true);
      File jf = new File(fileName);
      StramAppLauncher submitApp = new StramAppLauncher(jf);
      submitApp.loadDependencies();
      AppConfig appConfig = null;
      if (commandLineInfo.args.length >= 2) {
        File file = new File(commandLineInfo.args[1]);
        if (file.exists()) {
          appConfig = new StramAppLauncher.PropertyFileAppConfig(file);
        }
      }

      if (appConfig == null) {
        String matchString = commandLineInfo.args.length >= 2 ? commandLineInfo.args[1] : null;

        List<AppConfig> matchingAppConfigs = getMatchingAppConfigs(submitApp, matchString);
        if (matchingAppConfigs == null || matchingAppConfigs.isEmpty()) {
          throw new CliException("No matching applications bundled in jar.");
        }
        else if (matchingAppConfigs.size() == 1) {
          appConfig = matchingAppConfigs.get(0);
        }
        else if (matchingAppConfigs.size() > 1) {

          // Get app aliases. Figure out a better way to reuse aliases computed here in future.
          Map<String, String> appAliases = getAppAliases(config);

          // Display matching applications
          for (int i = 0; i < matchingAppConfigs.size(); i++) {
            String appName = matchingAppConfigs.get(i).getName();
            String className = appName.replace("/", ".").substring(0, appName.length()-6);
            String appAlias = appAliases.get(className);
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
              if (0 < option && option <= matchingAppConfigs.size()) {
                appConfig = matchingAppConfigs.get(option - 1);
              }
            }
            catch (Exception ex) {
              // ignore
            }
          }
        }

      }

      if (appConfig != null) {
        if (!commandLineInfo.localMode) {
          ApplicationId appId = submitApp.launchApp(appConfig, config);
          currentApp = rmClient.getApplicationReport(appId);
          System.out.println(appId);
        }
        else {
          submitApp.runLocal(appConfig, config);
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
        WebResource r = getPostResource(webServicesClient, app).path(StramWebServices.PATH_SHUTDOWN);
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

  }

  private class ListAppsCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      try {
        JSONObject singleKeyObj = new JSONObject();
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

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

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
            @SuppressWarnings("unchecked")
            Iterator<String> iterator = jsonObj.keys();

            while (iterator.hasNext()) {
              Object value = jsonObj.get(iterator.next());
              if (value.toString().matches("(?i).*" + args[1] + ".*")) {
                jsonArray.put(jsonObj);
                break;
              }
            }
          }
          else {
            jsonArray.put(jsonObj);
          }

        }
        singleKeyObj.put("apps", jsonArray);
        System.out.println(singleKeyObj.toString(2));
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
          app = getApplication(args[i]);
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
      updateCompleter(reader);
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
      if (topLevelHistory != null && changingLogicalPlanHistory != null) {
        topLevelHistory.flush();
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
      ClientResponse rsp = getResource(StramWebServices.PATH_CONTAINERS, currentApp);
      JSONObject json = rsp.getEntity(JSONObject.class);
      if (args.length == 1) {
        System.out.println(json.toString(2));
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
          for (int o = containers.length(); o-- > 0;) {
            JSONObject container = containers.getJSONObject(o);
            String id = container.getString("id");
            if (id != null && !id.isEmpty()) {
              for (int argc = args.length; argc-- > 1;) {
                String s1 = "0" + args[argc];
                String s2 = "_" + args[argc];
                if (id.equals(args[argc]) || id.endsWith(s1) || id.endsWith(s2)) {
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
      ClientResponse rsp = getResource(StramWebServices.PATH_OPERATORS, currentApp);
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
          @SuppressWarnings("unchecked")
          Iterator<String> keys = oper.keys();
          while (keys.hasNext()) {
            if (oper.get(keys.next()).toString().matches("(?i).*" + args[1] + ".*")) {
              matches.put(oper);
              break;
            }
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
      String containerLongId = getContainerLongId(args[1]);
      if (containerLongId == null) {
        throw new CliException("Container " + args[1] + " not found");
      }
      WebServicesClient webServicesClient = new WebServicesClient();
      WebResource r = getPostResource(webServicesClient, currentApp).path(StramWebServices.PATH_CONTAINERS).path(containerLongId).path("kill");
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
      WebResource r = getPostResource(webServicesClient, currentApp).path(StramWebServices.PATH_STARTRECORDING);
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
      WebResource r = getPostResource(webServicesClient, currentApp).path(StramWebServices.PATH_STOPRECORDING);
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
      WebResource r = getPostResource(webServicesClient, currentApp).path(StramWebServices.PATH_SYNCRECORDING);
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

  private class GetAppAttributesCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      if (currentApp == null) {
        throw new CliException("No application selected");
      }
      WebServicesClient webServicesClient = new WebServicesClient();
      WebResource r = getPostResource(webServicesClient, currentApp).path(StramWebServices.PATH_LOGICAL_PLAN).path("getAttributes");
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
        System.out.println(response.toString(2));
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
      WebResource r = getPostResource(webServicesClient, currentApp).path(StramWebServices.PATH_LOGICAL_PLAN_OPERATORS).path(args[1]).path("getAttributes");
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
        System.out.println(response.toString(2));
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
      WebResource r = getPostResource(webServicesClient, currentApp).path(StramWebServices.PATH_LOGICAL_PLAN_OPERATORS).path(args[1]).path(args[2]).path("getAttributes");
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
        System.out.println(response.toString(2));
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
      WebResource r = getPostResource(webServicesClient, currentApp).path(StramWebServices.PATH_LOGICAL_PLAN_OPERATORS).path(args[1]).path("getProperties");
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
        System.out.println(response.toString(2));
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
        WebResource r = getPostResource(webServicesClient, currentApp).path(StramWebServices.PATH_LOGICAL_PLAN_OPERATORS).path(args[1]).path("setProperty");
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
      if (args.length > 2) {
        String jarfile = expandFileName(args[1], true);
        String appName = args[2];
        File jf = new File(jarfile);
        StramAppLauncher submitApp = new StramAppLauncher(jf);
        submitApp.loadDependencies();
        List<AppConfig> matchingAppConfigs = getMatchingAppConfigs(submitApp, appName);
        if (matchingAppConfigs == null || matchingAppConfigs.isEmpty()) {
          throw new CliException("No application in jar file matches '" + appName + "'");
        }
        else if (matchingAppConfigs.size() > 1) {
          throw new CliException("More than one application in jar file match '" + appName + "'");
        }
        else {
          AppConfig appConfig = matchingAppConfigs.get(0);
          LogicalPlan logicalPlan = StramAppLauncher.prepareDAG(appConfig, StramAppLauncher.getConfig(null, null));
          ObjectMapper mapper = new ObjectMapper();
          System.out.println(new JSONObject(mapper.writeValueAsString(LogicalPlanSerializer.convertToMap(logicalPlan))).toString(2));
        }
      }
      else {
        if (currentApp == null) {
          throw new CliException("No application selected");
        }
        WebServicesClient webServicesClient = new WebServicesClient();
        WebResource r = getPostResource(webServicesClient, currentApp).path(StramWebServices.PATH_LOGICAL_PLAN);

        JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
          {
            return webResource.accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
          }

        });
        System.out.println(response.toString(2));
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
        File jf = new File(jarfile);
        StramAppLauncher submitApp = new StramAppLauncher(jf);
        submitApp.loadDependencies();
        List<AppConfig> matchingAppConfigs = getMatchingAppConfigs(submitApp, appName);
        if (matchingAppConfigs == null || matchingAppConfigs.isEmpty()) {
          throw new CliException("No application in jar file matches '" + appName + "'");
        }
        else if (matchingAppConfigs.size() > 1) {
          throw new CliException("More than one application in jar file match '" + appName + "'");
        }
        else {
          AppConfig appConfig = matchingAppConfigs.get(0);
          LogicalPlan logicalPlan = StramAppLauncher.prepareDAG(appConfig, StramAppLauncher.getConfig(null, null));
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
        WebResource r = getPostResource(webServicesClient, currentApp).path(StramWebServices.PATH_LOGICAL_PLAN);

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
      WebResource r = getPostResource(webServicesClient, currentApp).path(StramWebServices.PATH_LOGICAL_PLAN_MODIFICATION);
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
      reader.setHistory(topLevelHistory);
    }

  }

  private class ShowQueueCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      ObjectMapper mapper = new ObjectMapper();
      JSONObject singleKeyObj = new JSONObject();
      singleKeyObj.put("queue", new JSONArray(mapper.writeValueAsString(logicalPlanRequestQueue)));
      System.out.println(singleKeyObj.toString(2));
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

  private class GetAppInfoCommand implements Command
  {
    @Override
    public void execute(String[] args, ConsoleReader reader) throws Exception
    {
      ApplicationReport appReport = currentApp;
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
      WebResource r = getPostResource(webServicesClient, appReport).path(StramWebServices.PATH_INFO);

      JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
      {
        @Override
        public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
        {
          return webResource.accept(MediaType.APPLICATION_JSON).get(JSONObject.class);
        }

      });
      System.out.println(response.toString(2));
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

      WebServicesClient webServicesClient = new WebServicesClient();
      WebResource r = getPostResource(webServicesClient, currentApp).path(StramWebServices.PATH_ALERTS + "/" + args[1]);
      try {
        JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
          {
            return webResource.accept(MediaType.APPLICATION_JSON).put(clazz, json);
          }

        });
        System.out.println(response);
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
      WebResource r = getPostResource(webServicesClient, currentApp).path(StramWebServices.PATH_ALERTS + "/" + args[1]);
      try {
        JSONObject response = webServicesClient.process(r, JSONObject.class, new WebServicesClient.WebServicesHandler<JSONObject>()
        {
          @Override
          public JSONObject process(WebResource webResource, Class<JSONObject> clazz)
          {
            return webResource.accept(MediaType.APPLICATION_JSON).delete(clazz);
          }

        });
        System.out.println(response);
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
      System.out.println(json);
    }

  }

  public Map<String, String> getAppAliases(Configuration conf) {
    Map<String, String> aliases = new HashMap<String, String>();
    StringBuilder sb = new StringBuilder(DAGPropertiesBuilder.APPLICATION_PREFIX.replace(".", "\\."));
    sb.append("\\.(.*)\\.").append(DAGPropertiesBuilder.APPLICATION_CLASS.replace(".", "\\."));
    String appClassRegex = sb.toString();
    Map<String, String> props = conf.getValByRegex(appClassRegex);
    String appName = null;
    if (props != null) {
      Set<Map.Entry<String, String>> propEntries =  props.entrySet();
      for (Map.Entry<String, String> propEntry : propEntries) {
        Pattern p = Pattern.compile(appClassRegex);
        Matcher m = p.matcher(propEntry.getKey());
        if (m.find()) {
          appName = m.group(1);
          aliases.put(propEntry.getValue(), appName);
        }
      }
    }
    return aliases;
  }

  public static void main(String[] args) throws Exception
  {
    StramCli shell = new StramCli();
    shell.init(args);
    shell.run();
  }

}
