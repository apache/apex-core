/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import com.datatorrent.stram.engine.StreamingContainer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.DTLoggerFactory;

import com.datatorrent.api.StreamingApplication;

import com.datatorrent.stram.client.StramClientUtils;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.security.StramDelegationTokenIdentifier;
import com.datatorrent.stram.security.StramDelegationTokenManager;

/**
 *
 * Runnable to connect to the {@link ContainerManager} and launch the container that will host streaming operators<p>
 * <br>
 *
 * @since 0.3.2
 */
public class LaunchContainerRunnable implements Runnable
{
  private static final Logger LOG = LoggerFactory.getLogger(LaunchContainerRunnable.class);
  private static final String JAVA_REMOTE_DEBUG_OPTS = "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n";
  private final Map<String, String> containerEnv = new HashMap<String, String>();
  private final LogicalPlan dag;
  private final ByteBuffer tokens;
  private final Container container;
  private final NMClientAsync nmClient;

  /**
   * @param lcontainer Allocated container
   * @param nmClient
   * @param dag
   * @param tokens
   */
  public LaunchContainerRunnable(Container lcontainer, NMClientAsync nmClient, LogicalPlan dag, ByteBuffer tokens)
  {
    this.container = lcontainer;
    this.nmClient = nmClient;
    this.dag = dag;
    this.tokens = tokens;
  }

  private void setClasspath(Map<String, String> env)
  {
    // add localized application jar files to classpath
    // At some point we should not be required to add
    // the hadoop specific classpaths to the env.
    // It should be provided out of the box.
    // For now setting all required classpaths including
    // the classpath to "." for the application jar
    StringBuilder classPathEnv = new StringBuilder("./*");
    for (String c : nmClient.getConfig().get(YarnConfiguration.YARN_APPLICATION_CLASSPATH).split(",")) {
      if (c.equals("$HADOOP_CLIENT_CONF_DIR")) {
        // SPOI-2501
        continue;
      }
      classPathEnv.append(':');
      classPathEnv.append(c.trim());
    }
    classPathEnv.append(":."); // include log4j.properties, if any

    env.put("CLASSPATH", classPathEnv.toString());
    LOG.info("CLASSPATH: {}", classPathEnv);
  }

  public static void addFilesToLocalResources(LocalResourceType type, String commaSeparatedFileNames, Map<String, LocalResource> localResources, FileSystem fs) throws IOException
  {
    String[] files = StringUtils.splitByWholeSeparator(commaSeparatedFileNames, StramClient.LIB_JARS_SEP);
    for (String file : files) {
      Path dst = new Path(file);
      // Create a local resource to point to the destination jar path
      FileStatus destStatus = fs.getFileStatus(dst);
      LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
      // Set the type of resource - file or archive
      amJarRsrc.setType(type);
      // Set visibility of the resource
      // Setting to most private option
      amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
      // Set the resource to be copied over
      amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(dst));
      // Set timestamp and length of file so that the framework
      // can do basic sanity checks for the local resource
      // after it has been copied over to ensure it is the same
      // resource the client intended to use with the application
      amJarRsrc.setTimestamp(destStatus.getModificationTime());
      amJarRsrc.setSize(destStatus.getLen());
      localResources.put(dst.getName(), amJarRsrc);
    }
  }

  /**
   * Connects to CM, sets up container launch context and eventually dispatches the container start request to the CM.
   */
  @Override
  public void run()
  {
    LOG.info("Setting up container launch context for containerid={}", container.getId());
    ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

    setClasspath(containerEnv);
    try {
      // propagate to replace node managers user name (effective in non-secure mode)
      containerEnv.put("HADOOP_USER_NAME", UserGroupInformation.getLoginUser().getUserName());
    } catch (Exception e) {
      LOG.error("Failed to retrieve principal name", e);
    }
    // Set the environment
    ctx.setEnvironment(containerEnv);
    ctx.setTokens(tokens);


    // Set the local resources
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

    // add resources for child VM
    try {
      // child VM dependencies
      FileSystem fs = StramClientUtils.newFileSystemInstance(nmClient.getConfig());
      try {
        addFilesToLocalResources(LocalResourceType.FILE, dag.getAttributes().get(LogicalPlan.LIBRARY_JARS), localResources, fs);
        String files = dag.getAttributes().get(LogicalPlan.FILES);
        if (files != null) {
          addFilesToLocalResources(LocalResourceType.FILE, files, localResources, fs);
        }
        String archives = dag.getAttributes().get(LogicalPlan.ARCHIVES);
        if (archives != null) {
          addFilesToLocalResources(LocalResourceType.ARCHIVE, archives, localResources, fs);
        }
        ctx.setLocalResources(localResources);
      }
      finally {
        fs.close();
      }
    }
    catch (IOException e) {
      LOG.error("Failed to prepare local resources.", e);
      return;
    }

    // Set the necessary command to execute on the allocated container
    List<CharSequence> vargs = getChildVMCommand(container.getId().toString());

    // Get final command
    StringBuilder command = new StringBuilder(1024);
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }
    LOG.info("Launching on node: {} command: {}", container.getNodeId(), command);

    List<String> commands = new ArrayList<String>();
    commands.add(command.toString());
    ctx.setCommands(commands);

    nmClient.startContainerAsync(container, ctx);
  }

  /**
   * Build the command to launch the child VM in the container
   *
   * @param jvmID
   * @return
   */
  public List<CharSequence> getChildVMCommand(String jvmID)
  {

    List<CharSequence> vargs = new ArrayList<CharSequence>(8);

    if (!StringUtils.isBlank(System.getenv(Environment.JAVA_HOME.key()))) {
      // node manager provides JAVA_HOME
      vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
    }
    else {
      vargs.add("java");
    }

    String jvmOpts = dag.getAttributes().get(LogicalPlan.CONTAINER_JVM_OPTIONS);
    if (jvmOpts == null) {
      /* default Xmx based on total allocated memory size; default heap size 75% of total memory */
      vargs.add("-Xmx" + (container.getResource().getMemory() * 3 / 4) + "m");
      if (dag.isDebug()) {
        vargs.add(JAVA_REMOTE_DEBUG_OPTS);
      }
    }
    else {
      Map<String, String> params = new HashMap<String, String>();
      params.put("applicationId", Integer.toString(container.getId().getApplicationAttemptId().getApplicationId().getId()));
      params.put("containerId", Integer.toString(container.getId().getId()));
      StrSubstitutor sub = new StrSubstitutor(params, "%(", ")");
      vargs.add(sub.replace(jvmOpts));
      if (dag.isDebug() && !jvmOpts.contains("-agentlib:jdwp=")) {
        vargs.add(JAVA_REMOTE_DEBUG_OPTS);
      }
    }

    Path childTmpDir = new Path(Environment.PWD.$(),
                                YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
    vargs.add(String.format("-D%s=%s", StreamingContainer.PROP_APP_PATH, dag.assertAppPath()));
    vargs.add("-Djava.io.tmpdir=" + childTmpDir);
    vargs.add(String.format("-D%scid=%s", StreamingApplication.DT_PREFIX, jvmID));
    vargs.add("-Dhadoop.root.logger=" + (dag.isDebug() ? "DEBUG" : "INFO") + ",RFA");
    vargs.add("-Dhadoop.log.dir=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR);

    String loggersLevel = System.getProperty(DTLoggerFactory.DT_LOGGERS_LEVEL);
    if (loggersLevel != null) {
      vargs.add(String.format("-D%s=%s", DTLoggerFactory.DT_LOGGERS_LEVEL, loggersLevel));
    }
    // Add main class and its arguments
    vargs.add(StreamingContainer.class.getName());  // main of Child

    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

    // Final commmand
    StringBuilder mergedCommand = new StringBuilder(256);
    for (CharSequence str : vargs) {
      mergedCommand.append(str).append(" ");
    }
    List<CharSequence> vargsFinal = new ArrayList<CharSequence>(1);
    vargsFinal.add(mergedCommand.toString());
    return vargsFinal;

  }

  public static ByteBuffer getTokens(StramDelegationTokenManager delegationTokenManager, InetSocketAddress heartbeatAddress)
  {
    if (UserGroupInformation.isSecurityEnabled()) {
      Token<StramDelegationTokenIdentifier> stramToken;
      try {
        UserGroupInformation ugi = UserGroupInformation.getLoginUser();
        StramDelegationTokenIdentifier identifier = new StramDelegationTokenIdentifier(new Text(ugi.getUserName()), new Text(""), new Text(""));
        byte[] password = delegationTokenManager.addIdentifier(identifier);
        String service = heartbeatAddress.getAddress().getHostAddress() + ":" + heartbeatAddress.getPort();
        stramToken = new Token<StramDelegationTokenIdentifier>(identifier.getBytes(), password, identifier.getKind(), new Text(service));

        Collection<Token<? extends TokenIdentifier>> tokens = ugi.getTokens();
        Credentials credentials = new Credentials();
        for (Token<? extends TokenIdentifier> token : tokens) {
          if (!token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
            credentials.addToken(token.getService(), token);
            LOG.info("Passing container token {}", token);
          }
        }
        credentials.addToken(stramToken.getService(), stramToken);
        DataOutputBuffer dataOutput = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dataOutput);
        byte[] tokenBytes = dataOutput.getData();
        ByteBuffer cTokenBuf = ByteBuffer.wrap(tokenBytes);
        return cTokenBuf.duplicate();
      }
      catch (IOException e) {
        throw new RuntimeException("Error generating delegation token", e);
      }
    }
    return null;
  }

}
