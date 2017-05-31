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
package com.datatorrent.stram;

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

import org.apache.apex.engine.security.ACLManager;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.HadoopKerberosName;
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

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.client.StramClientUtils;
import com.datatorrent.stram.engine.StreamingContainer;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.security.StramDelegationTokenIdentifier;
import com.datatorrent.stram.security.StramDelegationTokenManager;

/**
 *
 * Runnable to connect to the {@link StreamingContainerManager} and launch the container that will host streaming operators<p>
 * <br>
 *
 * @since 0.3.2
 */
public class LaunchContainerRunnable implements Runnable
{
  private static final Logger LOG = LoggerFactory.getLogger(LaunchContainerRunnable.class);
  private static final String JAVA_REMOTE_DEBUG_OPTS = "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n";
  private final Map<String, String> containerEnv = new HashMap<>();
  private final LogicalPlan dag;
  private final ByteBuffer tokens;
  private final Container container;
  private final NMClientAsync nmClient;
  private final StreamingContainerAgent sca;
  private static final int MB_TO_B = 1024 * 1024;

  /**
   * @param lcontainer Allocated container
   * @param nmClient
   * @param sca
   * @param tokens
   */
  public LaunchContainerRunnable(Container lcontainer, NMClientAsync nmClient, StreamingContainerAgent sca, ByteBuffer tokens)
  {
    this.container = lcontainer;
    this.nmClient = nmClient;
    this.dag = sca.getContainer().getPlan().getLogicalPlan();
    this.tokens = tokens;
    this.sca = sca;
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
    String classpath = nmClient.getConfig().get(YarnConfiguration.YARN_APPLICATION_CLASSPATH);
    for (String c : StringUtils.isBlank(classpath) ? YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH : classpath.split(",")) {
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

  public static void addFileToLocalResources(final String name, final FileStatus fileStatus, final LocalResourceType type, final Map<String, LocalResource> localResources)
  {
    final LocalResource localResource = LocalResource.newInstance(ConverterUtils.getYarnUrlFromPath(fileStatus.getPath()),
        type, LocalResourceVisibility.APPLICATION, fileStatus.getLen(), fileStatus.getModificationTime());
    localResources.put(name, localResource);
  }

  public static void addFilesToLocalResources(LocalResourceType type, String commaSeparatedFileNames, Map<String, LocalResource> localResources, FileSystem fs) throws IOException
  {
    String[] files = StringUtils.splitByWholeSeparator(commaSeparatedFileNames, StramClient.LIB_JARS_SEP);
    for (String file : files) {
      final Path dst = new Path(file);
      addFileToLocalResources(dst.getName(), fs.getFileStatus(dst), type, localResources);
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

    // Setup ACLs for the impersonating user
    try {
      String launchPrincipal = System.getenv("HADOOP_USER_NAME");
      LOG.debug("Launch principal {}", launchPrincipal);
      if (launchPrincipal != null) {
        String launchUserName = launchPrincipal;
        if (UserGroupInformation.isSecurityEnabled()) {
          try {
            launchUserName = new HadoopKerberosName(launchPrincipal).getShortName();
          } catch (Exception ex) {
            LOG.warn("Error resolving kerberos principal {}", launchPrincipal, ex);
          }
        }
        LOG.debug("ACL launch user {} current user {}", launchUserName, UserGroupInformation.getCurrentUser().getShortUserName());
        if (!UserGroupInformation.getCurrentUser().getShortUserName().equals(launchUserName)) {
          ACLManager.setupUserACLs(ctx, launchUserName, nmClient.getConfig());
        }
      }
    } catch (IOException e) {
      LOG.warn("Unable to setup user acls for container {}", container.getId(), e);
    }
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
    Map<String, LocalResource> localResources = new HashMap<>();

    // add resources for child VM
    try {
      // child VM dependencies
      try (FileSystem fs = StramClientUtils.newFileSystemInstance(nmClient.getConfig())) {
        addFilesToLocalResources(LocalResourceType.FILE, dag.getAttributes().get(Context.DAGContext.LIBRARY_JARS), localResources, fs);
        String archives = dag.getAttributes().get(LogicalPlan.ARCHIVES);
        if (archives != null) {
          addFilesToLocalResources(LocalResourceType.ARCHIVE, archives, localResources, fs);
        }
        ctx.setLocalResources(localResources);
      }
    } catch (IOException e) {
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

    List<String> commands = new ArrayList<>();
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

    List<CharSequence> vargs = new ArrayList<>(8);

    if (!StringUtils.isBlank(System.getenv(Environment.JAVA_HOME.key()))) {
      // node manager provides JAVA_HOME
      vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
    } else {
      vargs.add("java");
    }

    String jvmOpts = dag.getAttributes().get(LogicalPlan.CONTAINER_JVM_OPTIONS);
    if (jvmOpts == null) {
      if (dag.isDebug()) {
        vargs.add(JAVA_REMOTE_DEBUG_OPTS);
      }
    } else {
      Map<String, String> params = new HashMap<>();
      params.put("applicationId", Integer.toString(container.getId().getApplicationAttemptId().getApplicationId().getId()));
      params.put("containerId", Integer.toString(container.getId().getId()));
      StrSubstitutor sub = new StrSubstitutor(params, "%(", ")");
      vargs.add(sub.replace(jvmOpts));
      if (dag.isDebug() && !jvmOpts.contains("-agentlib:jdwp=")) {
        vargs.add(JAVA_REMOTE_DEBUG_OPTS);
      }
    }

    List<DAG.OperatorMeta> operatorMetaList = Lists.newArrayList();
    int bufferServerMemory = 0;
    for (PTOperator operator : sca.getContainer().getOperators()) {
      bufferServerMemory += operator.getBufferServerMemory();
      operatorMetaList.add(operator.getOperatorMeta());
    }
    Context.ContainerOptConfigurator containerOptConfigurator = dag.getAttributes().get(LogicalPlan.CONTAINER_OPTS_CONFIGURATOR);
    jvmOpts = containerOptConfigurator.getJVMOptions(operatorMetaList);
    jvmOpts = parseJvmOpts(jvmOpts, ((long)bufferServerMemory) * MB_TO_B);
    LOG.info("Jvm opts {} for container {}",jvmOpts,container.getId());
    vargs.add(jvmOpts);

    Path childTmpDir = new Path(Environment.PWD.$(), YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
    vargs.add(String.format("-D%s=%s", StreamingContainer.PROP_APP_PATH, dag.assertAppPath()));
    vargs.add("-Djava.io.tmpdir=" + childTmpDir);
    vargs.add(String.format("-D%scid=%s", StreamingApplication.DT_PREFIX, jvmID));
    vargs.add("-Dhadoop.root.logger=" + (dag.isDebug() ? "DEBUG" : "INFO") + ",RFA");
    vargs.add("-Dhadoop.log.dir=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    StramClientUtils.addAttributeToArgs(LogicalPlan.APPLICATION_NAME, dag, vargs);
    StramClientUtils.addAttributeToArgs(LogicalPlan.LOGGER_APPENDER, dag, vargs);

    String loggersLevel = System.getProperty(StramUtils.DT_LOGGERS_LEVEL);
    if (loggersLevel != null) {
      vargs.add(String.format("-D%s=%s", StramUtils.DT_LOGGERS_LEVEL, loggersLevel));
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
    List<CharSequence> vargsFinal = new ArrayList<>(1);
    vargsFinal.add(mergedCommand.toString());
    return vargsFinal;

  }

  private String parseJvmOpts(String jvmOpts, long memory)
  {
    String xmx = "-Xmx";
    StringBuilder builder = new StringBuilder();
    if (jvmOpts != null && jvmOpts.length() > 1) {
      String[] splits = jvmOpts.split("(\\s+)");
      boolean foundProperty = false;
      for (String split : splits) {
        if (split.startsWith(xmx)) {
          foundProperty = true;
          long heapSize = Long.valueOf(split.substring(xmx.length()));
          heapSize += memory;
          builder.append(xmx).append(heapSize).append(" ");
        } else {
          builder.append(split).append(" ");
        }
      }
      if (!foundProperty) {
        builder.append(xmx).append(memory);
      }
    }
    return builder.toString();
  }

  public static ByteBuffer getTokens(StramDelegationTokenManager delegationTokenManager, InetSocketAddress heartbeatAddress) throws IOException
  {
    if (UserGroupInformation.isSecurityEnabled()) {
      UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      StramDelegationTokenIdentifier identifier = new StramDelegationTokenIdentifier(new Text(ugi.getUserName()), new Text(""), new Text(""));
      String service = heartbeatAddress.getAddress().getHostAddress() + ":" + heartbeatAddress.getPort();
      Token<StramDelegationTokenIdentifier> stramToken = new Token<>(identifier, delegationTokenManager);
      stramToken.setService(new Text(service));
      return getTokens(ugi, stramToken);
    }
    return null;
  }

  public static ByteBuffer getTokens(UserGroupInformation ugi, Token<StramDelegationTokenIdentifier> delegationToken)
  {
    try {
      Collection<Token<? extends TokenIdentifier>> tokens = ugi.getCredentials().getAllTokens();
      Credentials credentials = new Credentials();
      for (Token<? extends TokenIdentifier> token : tokens) {
        if (!token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
          credentials.addToken(token.getService(), token);
          LOG.debug("Passing container token {}", token);
        }
      }
      credentials.addToken(delegationToken.getService(), delegationToken);
      DataOutputBuffer dataOutput = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dataOutput);
      byte[] tokenBytes = dataOutput.getData();
      ByteBuffer cTokenBuf = ByteBuffer.wrap(tokenBytes);
      return cTokenBuf.duplicate();
    } catch (IOException e) {
      throw new RuntimeException("Error generating delegation token", e);
    }
  }
}
