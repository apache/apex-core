/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram.webapp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.util.logging.Level;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.google.inject.util.Providers;
import com.malhartech.stram.StramAppContext;
import com.malhartech.stram.StreamingContainerManager;
import com.malhartech.stram.webapp.StramWebApp.JAXBContextResolver;
import com.malhartech.stream.StramTestSupport;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;

/**
 * Test the application master web services api's.
 * Also test non-existent urls.
 * Adapted from MR.
 *
 *  /ws/v1/stram
 *  /ws/v1/stram/info
 */
public class StramWebServicesTest extends JerseyTest {

  private static Configuration conf = new Configuration();
  private static TestAppContext appContext;

  static class TestAppContext implements StramAppContext {
    final ApplicationAttemptId appAttemptID;
    final ApplicationId appID;
    final String appPath = "/testPath";
    final String userId = "testUser";
    final long startTime = System.currentTimeMillis();

    TestAppContext(int appid, int numJobs, int numTasks, int numAttempts) {
      this.appID = Records.newRecord(ApplicationId.class);
      this.appID.setId(appid);
      this.appAttemptID = Records.newRecord(ApplicationAttemptId.class);
      this.appAttemptID.setApplicationId(this.appID);
      this.appAttemptID.setAttemptId(numAttempts);
    }

    TestAppContext() {
      this(0, 1, 1, 1);
    }

    @Override
    public ApplicationAttemptId getApplicationAttemptId() {
      return appAttemptID;
    }

    @Override
    public ApplicationId getApplicationID() {
      return appID;
    }

    @Override
    public String getApplicationPath()
    {
      return appPath;
    }

    @Override
    public String getAppMasterTrackingUrl() {
      return "unknown";
    }

    @Override
    public CharSequence getUser() {
      return userId;
    }

    @Override
    public Clock getClock() {
      return null;
    }

    @Override
    public String getApplicationName() {
      return "TestApp";
    }

    @Override
    public long getStartTime() {
      return startTime;
    }

    @Override
    public AppInfo.AppStats getStats() {
      return new AppInfo.AppStats() {
      };
    }
  }

  public static class SomeStats {
    public int field1 = 2;
  }

  public static class GuiceServletConfig extends GuiceServletContextListener {
    // new instance needs to be created for each test
    private final Injector injector = Guice.createInjector(new ServletModule() {
      @Override
      protected void configureServlets() {

        appContext = new TestAppContext();
        bind(JAXBContextResolver.class);
        bind(StramWebServices.class);
        bind(GenericExceptionHandler.class);
        bind(StramAppContext.class).toInstance(appContext);
        bind(StreamingContainerManager.class).toProvider(Providers.<StreamingContainerManager>of(null));
        bind(Configuration.class).toInstance(conf);
        serve("/*").with(GuiceContainer.class);
      }
    });

    @Override
    protected Injector getInjector() {
      return injector;
    }
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public StramWebServicesTest() {
    super(new WebAppDescriptor.Builder(
        StramWebServicesTest.class.getPackage().getName())
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  public void testAM() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path(StramWebServices.PATH)
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyAMInfo(json.getJSONObject(StramWebServices.PATH_INFO), appContext);
  }

  @Test
  public void testAMSlash() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path(StramWebServices.PATH + "/")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyAMInfo(json.getJSONObject(StramWebServices.PATH_INFO), appContext);
  }

  @Test
  public void testAMDefault() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path(StramWebServices.PATH + "/")
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyAMInfo(json.getJSONObject(StramWebServices.PATH_INFO), appContext);
  }

  @Test
  public void testAMXML() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path(StramWebServices.PATH)
        .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    String xml = response.getEntity(String.class);
    verifyAMInfoXML(xml, appContext);
  }

  @Test
  public void testInfo() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path(StramWebServices.PATH)
        .path(StramWebServices.PATH_INFO).accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyAMInfo(json.getJSONObject(StramWebServices.PATH_INFO), appContext);
  }

  @Test
  public void testInfoSlash() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path(StramWebServices.PATH)
        .path(StramWebServices.PATH_INFO + "/").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyAMInfo(json.getJSONObject(StramWebServices.PATH_INFO), appContext);
  }

  @Test
  public void testInfoDefault() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path(StramWebServices.PATH)
        .path(StramWebServices.PATH_INFO + "/").get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyAMInfo(json.getJSONObject(StramWebServices.PATH_INFO), appContext);
  }

  @Test
  public void testInfoXML() throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response = r.path(StramWebServices.PATH)
        .path(StramWebServices.PATH_INFO + "/").accept(MediaType.APPLICATION_XML)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    String xml = response.getEntity(String.class);
    verifyAMInfoXML(xml, appContext);
  }

  @Test
  @SuppressWarnings("UnusedAssignment")
  public void testInvalidUri() throws JSONException, Exception {
    WebResource r = resource();
    String responseStr = "";
    try {
      responseStr = r.path(StramWebServices.PATH).path("bogus")
          .accept(MediaType.APPLICATION_JSON).get(String.class);
      fail("should have thrown exception on invalid uri");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertEquals(Status.NOT_FOUND, response.getClientResponseStatus());
      StramTestSupport.checkStringMatch(
          "error string exists and shouldn't", "", responseStr);
    }
  }

  @Test
  @SuppressWarnings("UnusedAssignment")
  public void testInvalidUri2() throws JSONException, Exception {
    WebResource r = resource();
    String responseStr = "";
    try {
      responseStr = r.path("ws").path("v1").path("invalid")
          .accept(MediaType.APPLICATION_JSON).get(String.class);
      fail("should have thrown exception on invalid uri");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertEquals(Status.NOT_FOUND, response.getClientResponseStatus());
      StramTestSupport.checkStringMatch(
          "error string exists and shouldn't", "", responseStr);
    }
  }

  @Test
  @SuppressWarnings("UnusedAssignment")
  public void testInvalidAccept() throws JSONException, Exception {
    // suppress logging in jersey to get rid of expected stack traces from test log
    java.util.logging.Logger.getLogger("org.glassfish.grizzly.servlet.ServletHandler").setLevel(Level.OFF);
    java.util.logging.Logger.getLogger("com.sun.jersey.spi.container.ContainerResponse").setLevel(Level.OFF);

    WebResource r = resource();
    String responseStr = "";
    try {
      responseStr = r.path(StramWebServices.PATH)
          .accept(MediaType.TEXT_PLAIN).get(String.class);
      fail("should have thrown exception on invalid accept");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertEquals(Status.INTERNAL_SERVER_ERROR,
          response.getClientResponseStatus());
      StramTestSupport.checkStringMatch(
          "error string exists and shouldn't", "", responseStr);
    }
  }

  void verifyAMInfo(JSONObject info, TestAppContext ctx)
      throws JSONException {
    assertEquals("incorrect number of elements", 8, info.length());

    verifyAMInfoGeneric(ctx, info.getString("appId"), info.getString("user"),
        info.getString("name"), info.getLong("startedOn"),
        info.getLong("elapsedTime"));
  }

   void verifyAMInfoXML(String xml, TestAppContext ctx)
      throws JSONException, Exception {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName(StramWebServices.PATH_INFO);
    assertEquals("incorrect number of elements", 1, nodes.getLength());

    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);
      verifyAMInfoGeneric(ctx,
          getXmlString(element, "appId"),
          getXmlString(element, "user"),
          getXmlString(element, "name"),
          getXmlLong(element, "startedOn"),
          getXmlLong(element, "elapsedTime"));
    }
  }

  void verifyAMInfoGeneric(TestAppContext ctx, String id, String user,
      String name, long startedOn, long elapsedTime) {

    StramTestSupport.checkStringMatch("id", ctx.getApplicationID()
        .toString(), id);
    StramTestSupport.checkStringMatch("user", ctx.getUser().toString(),
        user);
    StramTestSupport.checkStringMatch("name", ctx.getApplicationName(),
        name);

    assertEquals("startedOn incorrect", ctx.getStartTime(), startedOn);
    assertTrue("elapsedTime not greater then 0", (elapsedTime > 0));

  }

  public static String getXmlString(Element element, String name) {
    NodeList id = element.getElementsByTagName(name);
    Element line = (Element) id.item(0);
    if (line == null) {
      return null;
    }
    Node first = line.getFirstChild();
    // handle empty <key></key>
    if (first == null) {
      return "";
    }
    String val = first.getNodeValue();
    if (val == null) {
      return "";
    }
    return val;
  }

  public static long getXmlLong(Element element, String name) {
    String val = getXmlString(element, name);
    return Long.parseLong(val);
  }

}
