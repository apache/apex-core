package com.datatorrent.stram;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.datatorrent.api.DAG;
import com.datatorrent.common.util.AsyncFSStorageAgent;
import com.datatorrent.netlet.util.DTThrowable;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.support.StramTestSupport;

public class StreamingAppMasterServiceTest
{
  private static File CLUSTER_WORK_DIR = new File("target", StreamingAppMasterServiceTest.class.getName());
  StreamingAppMasterService appMaster;
  StreamingAppMasterService mockedAppMaster;
  Configuration conf;
  int containerCount = 1;
  ApplicationAttemptId appAttemptID;
  int priority = 0;
  List<ContainerId> containerIds = new ArrayList<>();
  List<String> containerHostnames = Arrays.asList("node1", "node1", "node1", "node2");
  List<Integer> containerExitCodes = Arrays.asList(-100, -100, -100, -100);
  Iterator<String> hostnameIterator;
  Iterator<Integer> exitCodeIterator;

  @Before
  public void setupEachTime() throws Exception
  {
    hostnameIterator = containerHostnames.iterator();
    exitCodeIterator = containerExitCodes.iterator();

    StramAppContext appContext = new StramTestSupport.TestAppContext(1, 10, 10, 10);
    appAttemptID = appContext.getApplicationAttemptId();
    appMaster = new StreamingAppMasterService(appAttemptID);
    conf = new YarnConfiguration();

    RegisterApplicationMasterResponse response = mock(RegisterApplicationMasterResponse.class);
    Resource resource = mock(Resource.class);
    when(resource.getMemory()).thenReturn(1024);
    when(resource.getVirtualCores()).thenReturn(10);

    when(response.getMaximumResourceCapability()).thenReturn(resource);

    ResourceRequestHandler resourceRequestor = new ResourceRequestHandler();
    AMRMClientImpl<ContainerRequest> client = mock(AMRMClientImpl.class);
    AllocateResponse allocateResponse = mock(AllocateResponse.class);
    when(client.allocate(0)).thenReturn(allocateResponse);

    doReturn(response).when(client).registerApplicationMaster(any(String.class), any(int.class), any(String.class));

    doAnswer(new Answer<List<ContainerStatus>>()
    {
      public List<ContainerStatus> answer(InvocationOnMock invocation)
      {
        return getListOfContainerStatuses();
      }
    }).when(allocateResponse).getCompletedContainersStatuses();

    doAnswer(new Answer<List<Container>>()
    {
      public List<Container> answer(InvocationOnMock invocation)
      {
        return getListOfContainers();
      }
    }).when(allocateResponse).getAllocatedContainers();

    appMaster.setAmRmClient(client);

    mockedAppMaster = spy(appMaster);
    LaunchContainerRunnable obj = mock(LaunchContainerRunnable.class);

    doReturn(obj).when(mockedAppMaster).createLaunchContainerRunnable(any(Container.class), any(StreamingContainerAgent.class), any(ByteBuffer.class));

    doReturn(resourceRequestor).when(mockedAppMaster).createResourceRequestor();
    doReturn(true).when(mockedAppMaster).setupRMService(any(Configuration.class), any(int.class), any(ResourceRequestHandler.class));
    when(mockedAppMaster.getConfig()).thenReturn(conf);
  }

  @After
  public void teardown()
  {
  }

  @Test
  public void testBlacklistingOfFailedNodes() throws Exception
  {
    LogicalPlan dag = setupDag();
    dag.setAttribute(LogicalPlan.MAX_CONSECUTIVE_CONTAINER_FAILURES, 3);

    Thread thread = new Thread("LocalAMService")
    {
      @Override
      public void run()
      {
        long startTms = System.currentTimeMillis();
        long timeout = 50000L;
        try {
          while (mockedAppMaster.getBlacklistedNodes().isEmpty() && System.currentTimeMillis() - startTms < timeout) {
            Thread.sleep(500);
          }
        } catch (InterruptedException ex) {
          DTThrowable.rethrow(ex);
        } finally {
          mockedAppMaster.setAppDone(true);
        }
      }
    };
    thread.start();

    mockedAppMaster.run();
    thread.join();
    Assert.assertEquals("Node1 should be blacklisted after 3 consecutive failures", "node1", mockedAppMaster.getBlacklistedNodes().get(0));
    Assert.assertEquals("Node1 should have 3 failures marked", 3, mockedAppMaster.getFailedContainersMap().get("node1").get());
  }

  @Test
  public void testBlacklistedNodesAreRemovedAfterTimeout() throws Exception
  {
    LogicalPlan dag = setupDag();
    dag.setAttribute(LogicalPlan.BLACKLIST_REMOVAL_TIME, new Long(0));

    final Thread thread = new Thread("LocalAMService")
    {
      @Override
      public void run()
      {
        long startTms = System.currentTimeMillis();
        long timeout = 50000L;
        try {
          while (mockedAppMaster.getBlacklistedNodes().isEmpty() && System.currentTimeMillis() - startTms < timeout) {
            Thread.sleep(500);
          }
        } catch (InterruptedException e) {
          DTThrowable.rethrow(e);
        }
      }
    };
    thread.start();

    Thread thread1 = new Thread("LocalAMService1")
    {
      @Override
      public void run()
      {
        try {
          thread.join();
          long startTms = System.currentTimeMillis();
          long timeout = 1000L;

          // Check that node1 is removed from blacklist immediately
          while (!mockedAppMaster.getBlacklistedNodes().isEmpty() && System.currentTimeMillis() - startTms < timeout) {
            Thread.sleep(500);
          }
        } catch (InterruptedException e) {
          DTThrowable.rethrow(e);
        } finally {
          mockedAppMaster.setAppDone(true);
        }
      }
    };
    thread1.start();
    mockedAppMaster.run();
    thread1.join();
    Assert.assertTrue("Node1 should be removed from blacklist and blacklisted node's list should be empty", mockedAppMaster.getBlacklistedNodes().isEmpty());
  }

  @Test
  public void testFailedCountForNodeIsResetOnSuccess() throws Exception
  {
    LogicalPlan dag = setupDag();
    dag.setAttribute(LogicalPlan.MAX_CONSECUTIVE_CONTAINER_FAILURES, 4);

    // Return node1 4 times, exit code should be 0 for the 4th one
    containerHostnames = Arrays.asList("node1", "node1", "node1", "node1");
    containerExitCodes = Arrays.asList(-100, -100, -100, 0);
    hostnameIterator = containerHostnames.iterator();
    exitCodeIterator = containerExitCodes.iterator();

    Thread thread = new Thread("LocalAMService")
    {
      @Override
      public void run()
      {
        long startTms = System.currentTimeMillis();
        long timeout = 50000L;
        try {
          while (!mockedAppMaster.getFailedContainersMap().containsKey("node1") && System.currentTimeMillis() - startTms < timeout) {
            Thread.sleep(500);
          }
          Assert.assertTrue("Node 1 number of failures are set ", mockedAppMaster.getFailedContainersMap().containsKey("node1"));
          startTms = System.currentTimeMillis();
          while (mockedAppMaster.getFailedContainersMap().get("node1").get() < 3 && System.currentTimeMillis() - startTms < timeout) {
            Thread.sleep(500);
          }
          System.out.println("Number of failures = " +  mockedAppMaster.getFailedContainersMap().get("node1").get());
          startTms = System.currentTimeMillis();
          timeout = 5000L;
          Assert.assertEquals("Node 1 number of failures should be 3 ", 3, mockedAppMaster.getFailedContainersMap().get("node1").get());
          while (mockedAppMaster.getFailedContainersMap().get("node1").get() != 0 && System.currentTimeMillis() - startTms < timeout) {
            Thread.sleep(500);
          }
        } catch (InterruptedException ex) {
          DTThrowable.rethrow(ex);
        } finally {
          mockedAppMaster.setAppDone(true);
        }
      }
    };
    thread.start();

    mockedAppMaster.run();
    thread.join();
    Assert.assertEquals("Node1 should have 0 failures marked", 0, mockedAppMaster.getFailedContainersMap().get("node1").get());
  }

  private LogicalPlan setupDag()
  {
    LogicalPlan dag = new LogicalPlan();
    String basePath = "target/";
    String applicationPath = basePath + "/app";
    try {
      FileUtils.forceMkdir(new File(basePath));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    dag.setAttribute(DAG.APPLICATION_PATH, applicationPath);

    mockedAppMaster.setDag(dag);
    dag.validate();

    String pathUri = CLUSTER_WORK_DIR.toURI().toString();

    dag.getAttributes().put(LogicalPlan.APPLICATION_ID, "app_local_" + System.currentTimeMillis());
    if (dag.getAttributes().get(LogicalPlan.APPLICATION_PATH) == null) {
      dag.getAttributes().put(LogicalPlan.APPLICATION_PATH, pathUri);
    }
    if (dag.getAttributes().get(OperatorContext.STORAGE_AGENT) == null) {
      dag.setAttribute(OperatorContext.STORAGE_AGENT, new AsyncFSStorageAgent(new Path(pathUri, LogicalPlan.SUBDIR_CHECKPOINTS).toString(), null));
    }
    dag.addOperator("ABC", new TestGeneratorInputOperator());
    mockedAppMaster.setStreamingContainerAgent(new StreamingContainerManager(dag));
    return dag;
  }

  private List<Container> getListOfContainers()
  {
    List<Container> containers = new ArrayList<>();
    if (hostnameIterator.hasNext()) {
      String hostname = hostnameIterator.next();
      for (int i = 0; i < containerCount; i++) {
        ContainerId id = ContainerId.newInstance(appAttemptID, priority);
        containerIds.add(id);
        NodeId nodeId = NodeId.newInstance(hostname, 9090);
        Container newContainer = Container.newInstance(id, nodeId, "0.10.0.10", Resource.newInstance(10, 1024), Priority.newInstance(priority++), Token.newInstance("abc".getBytes(), "abc", "abc".getBytes(), "abc"));
        containers.add(newContainer);
      }
    }
    return containers;
  }

  private List<ContainerStatus> getListOfContainerStatuses()
  {
    List<ContainerStatus> containerStatuses = new ArrayList<>();
    Integer existStatus = 0;
    if (exitCodeIterator.hasNext()) {
      existStatus = exitCodeIterator.next();

      for (int i = 0; i < containerCount; i++) {
        ContainerStatus containerStatus = mock(ContainerStatus.class);
        when(containerStatus.getContainerId()).thenReturn(containerIds.get(containerIds.size() - containerCount + i));
        when(containerStatus.getState()).thenReturn(ContainerState.COMPLETE);
        when(containerStatus.getExitStatus()).thenReturn(existStatus);

        containerStatuses.add(containerStatus);
      }
    }
    return containerStatuses;
  }
}
