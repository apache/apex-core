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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.rule.PowerMockRule;
import org.powermock.reflect.Whitebox;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;

import com.datatorrent.stram.client.StramAgent;
import com.datatorrent.stram.util.WebServicesClient;

import jline.console.ConsoleReader;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.support.membermodification.MemberMatcher.constructor;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

/**
 *
 */
@PowerMockIgnore({
    "javax.net.ssl.*",
    "org.apache.log4j.*"
    })
@PrepareForTest({YarnClient.class, ApexCli.class, StramAgent.class})
public class ApexCliShutdownCommandTest
{

  @Rule
  public PowerMockRule powerMockRule = new PowerMockRule();

  private ApplicationReport mockRunningApplicationReport(String appId, String appName)
  {
    ApplicationReport app = mock(ApplicationReport.class);
    ApplicationId applicationId = mock(ApplicationId.class);

    when(applicationId.toString()).thenReturn(appId);
    when(app.getApplicationId()).thenReturn(applicationId);

    when(app.getName()).thenReturn(appName);

    when(app.getYarnApplicationState()).thenReturn(YarnApplicationState.RUNNING);
    when(app.getFinalApplicationStatus()).thenReturn(FinalApplicationStatus.UNDEFINED);

    when(app.getTrackingUrl()).thenReturn("http://example.com");

    return app;
  }

  @Test
  public void shutdownAppCommandUsesBestEffortApproach() throws Exception
  {
    // Given a cli and two running apps
    ApexCli cliUnderTest = new ApexCli();
    StramAgent stramAgent = mock(StramAgent.class);
    YarnClient yarnClient = mock(YarnClient.class);

    Whitebox.setInternalState(cliUnderTest, "stramAgent", stramAgent);
    Whitebox.setInternalState(cliUnderTest, "yarnClient", yarnClient);
    Whitebox.setInternalState(cliUnderTest, "consolePresent", true);

    suppress(constructor(WebServicesClient.class, new Class[0]));

    List<ApplicationReport> runningApplications = new ArrayList<>();

    ApplicationReport app1 = mockRunningApplicationReport("application-id-1", "app1");
    ApplicationReport app2 = mockRunningApplicationReport("application-id-2", "app2");

    runningApplications.add(app1);
    runningApplications.add(app2);

    when(yarnClient.getApplications(Mockito.any(Set.class))).thenReturn(runningApplications);
    when(stramAgent.issueStramWebRequest(
        Mockito.any(WebServicesClient.class),
        Mockito.anyString(),
        Mockito.any(StramAgent.StramUriSpec.class),
        Mockito.any(WebServicesClient.WebServicesHandler.class)))
        .thenReturn(new JSONObject());

    final ByteArrayOutputStream stdOut = new ByteArrayOutputStream();
    final ByteArrayOutputStream stdErr = new ByteArrayOutputStream();

    PrintStream beforeOut = System.out;
    PrintStream beforeErr = System.err;

    System.setOut(new PrintStream(stdOut, true));
    System.setErr(new PrintStream(stdErr, true));

    // When processing the shutdown command for two valid and one invalid appNames
    String shutdownAppsCommand = "shutdown-app app1 notExisting app2";
    cliUnderTest.processLine(shutdownAppsCommand, new ConsoleReader(), true);

    // Then the output contains two success and one error messages
    Assert.assertEquals(
        "Shutdown of app application-id-1 requested: {}\nShutdown of app application-id-2 requested: {}\n",
        stdOut.toString()
    );

    Assert.assertEquals(
        "Failed to request shutdown for app notExisting: Application with id or name notExisting not found\n",
        stdErr.toString()
    );

    System.setOut(beforeOut);
    System.setErr(beforeErr);
  }
}
