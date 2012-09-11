/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.malhartech.stram.cli.StramClientUtils.YarnClientHelper;
import com.malhartech.stram.conf.Topology;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Runnable to connect to the {@link ContainerManager} and launch the container that will host streaming nodes<p>
 * <br>
 */
public class LaunchContainerRunnable implements Runnable
{
  private static final transient Logger LOG = LoggerFactory.getLogger(LaunchContainerRunnable.class);
  private final YarnClientHelper yarnClient;
  private final Map<String, String> containerEnv = new HashMap<String, String>();
  private final InetSocketAddress heartbeatAddress;
  private final Topology topology;
  private final Container container;

  /**
   * @param lcontainer Allocated container
   */
  public LaunchContainerRunnable(Container lcontainer, YarnClientHelper yarnClient, Topology topology, InetSocketAddress heartbeatAddress)
  {
    this.container = lcontainer;
    this.yarnClient = yarnClient;
    this.heartbeatAddress = heartbeatAddress;
    this.topology = topology;
  }

  private void setClasspath(Map<String, String> env)
  {
    // add localized application jar files to classpath
    // At some point we should not be required to add
    // the hadoop specific classpaths to the env.
    // It should be provided out of the box.
    // For now setting all required classpaths including
    // the classpath to "." for the application jar
    StringBuilder classPathEnv = new StringBuilder("${CLASSPATH}:./*");
    for (String c : yarnClient.getConf().get(YarnConfiguration.YARN_APPLICATION_CLASSPATH).split(",")) {
      classPathEnv.append(':');
      classPathEnv.append(c.trim());
    }
    classPathEnv.append(":."); // include log4j.properties, if any

    env.put("CLASSPATH", classPathEnv.toString());
    LOG.info("CLASSPATH: {}", classPathEnv);
  }

  public static void addLibJarsToLocalResources(String libJars, Map<String, LocalResource> localResources, FileSystem fs) throws IOException
  {
    String[] jarPathList = StringUtils.splitByWholeSeparator(libJars, ",");
    for (String jarPath : jarPathList) {
      Path dst = new Path(jarPath);
      // Create a local resource to point to the destination jar path
      FileStatus destStatus = fs.getFileStatus(dst);
      LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
      // Set the type of resource - file or archive
      amJarRsrc.setType(LocalResourceType.FILE);
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

  @Override
  /**
   * Connects to CM, sets up container launch context for shell command and eventually dispatches the container start request to the CM.
   */
  public void run()
  {
    // Connect to ContainerManager
    ContainerManager cm = yarnClient.connectToCM(container);

    LOG.info("Setting up container launch container for containerid=" + container.getId());
    ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

    ctx.setContainerId(container.getId());
    ctx.setResource(container.getResource());

    try {
      ctx.setUser(UserGroupInformation.getCurrentUser().getShortUserName());
    }
    catch (IOException e) {
      LOG.info("Getting current user info failed when trying to launch the container"
               + e.getMessage());
    }

    setClasspath(containerEnv);
    // Set the environment
    ctx.setEnvironment(containerEnv);

    // Set the local resources
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

    // add resources for child VM
    try {
      // child VM dependencies
      FileSystem fs = FileSystem.get(yarnClient.getConf());
      addLibJarsToLocalResources(topology.getLibJars(), localResources, fs);
      ctx.setLocalResources(localResources);
    }
    catch (IOException e) {
      LOG.error("Failed to prepare local resources.", e);
      return;
    }

    // Set the necessary command to execute on the allocated container
    List<CharSequence> vargs = getChildVMCommand(container.getId().toString());

    // Get final commmand
    StringBuilder command = new StringBuilder();
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }
    LOG.info("Final command is: {}", command);

    List<String> commands = new ArrayList<String>();
    commands.add(command.toString());
    ctx.setCommands(commands);

    StartContainerRequest startReq = Records.newRecord(StartContainerRequest.class);
    startReq.setContainerLaunchContext(ctx);
    try {
      cm.startContainer(startReq);
    }
    catch (YarnRemoteException e) {
      LOG.error("Start container failed for :"
                + ", containerId=" + container.getId());
      e.printStackTrace();
      // TODO do we need to release this container?
    }

    // Get container status?
    // Left commented out as the shell scripts are short lived
    // and we are relying on the status for completed containers from RM to detect status

    //    GetContainerStatusRequest statusReq = Records.newRecord(GetContainerStatusRequest.class);
    //    statusReq.setContainerId(container.getId());
    //    GetContainerStatusResponse statusResp;
    //try {
    //statusResp = cm.getContainerStatus(statusReq);
    //    LOG.info("Container Status"
    //    + ", id=" + container.getId()
    //    + ", status=" +statusResp.getStatus());
    //} catch (YarnRemoteException e) {
    //e.printStackTrace();
    //}
  }

  /**
   * Build the command to launch the child VM in the container TODO: Build based on streaming node configuration
   *
   * @param jvmID
   * @return List<CharSequence>
   */
  public List<CharSequence> getChildVMCommand(
    String jvmID)
  {

    List<CharSequence> vargs = new ArrayList<CharSequence>(8);

    //vargs.add("exec");
    if (!StringUtils.isBlank(System.getenv(Environment.JAVA_HOME.$()))) {
      vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
    }
    else {
      vargs.add("java");
    }

    if (topology.isDebug()) {
      vargs.add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n");
    }
    // TODO: heap size - VM may use more memory and the container may get killed
    // Set Xmx based on am memory size
    vargs.add("-Xmx" + container.getResource().getMemory() + "m");

    Path childTmpDir = new Path(Environment.PWD.$(),
                                YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
    vargs.add("-Djava.io.tmpdir=" + childTmpDir);

    // Add main class and its arguments
    vargs.add(StramChild.class.getName());  // main of Child
    // pass listener's address
    vargs.add(heartbeatAddress.getAddress().getHostAddress());
    vargs.add(Integer.toString(heartbeatAddress.getPort()));

    // Finally add the jvmID
    vargs.add(String.valueOf(jvmID));
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

    // Final commmand
    StringBuilder mergedCommand = new StringBuilder();
    for (CharSequence str : vargs) {
      mergedCommand.append(str).append(" ");
    }
    List<CharSequence> vargsFinal = new ArrayList<CharSequence>(1);
    vargsFinal.add(mergedCommand.toString());
    return vargsFinal;

  }
}
