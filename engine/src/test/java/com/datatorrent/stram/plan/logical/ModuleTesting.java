package com.datatorrent.stram.plan.logical;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.plan.logical.TestModules.RandGen;
import com.datatorrent.stram.plan.logical.TestModules.RandGenModule;
import com.datatorrent.stram.plan.logical.TestModules.WrapperModule;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.Valid;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ModuleTesting
{
  @Test
  public void testModuleProperties() {

    Configuration conf = new Configuration(false);
    conf.set(StreamingApplication.DT_PREFIX + "module.o1.prop.myStringProperty", "myStringPropertyValue");
    conf.set(StreamingApplication.DT_PREFIX + "module.o2.prop.stringArrayField", "a,b,c");
    conf.set(StreamingApplication.DT_PREFIX + "module.o2.prop.mapProperty.key1", "key1Val");
    conf.set(StreamingApplication.DT_PREFIX + "module.o2.prop.mapProperty(key1.dot)", "key1dotVal");
    conf.set(StreamingApplication.DT_PREFIX + "module.o2.prop.mapProperty(key2.dot)", "key2dotVal");

    LogicalPlan dag = new LogicalPlan();
    TestModules.GenericModule o1 = dag.addModule("o1", new TestModules.GenericModule());
    //LogicalPlanTest.ValidationTestOperator o2 = dag.addOperator("o2", new LogicalPlanTest.ValidationTestOperator());
    ValidationTestModule o2 = dag.addModule("o2", new ValidationTestModule());

    LogicalPlanConfiguration pb = new LogicalPlanConfiguration(conf);

    pb.setModuleProperties(dag, "testSetOperatorProperties");
    System.out.println("setted module properties");
    Assert.assertEquals("o1.myStringProperty", "myStringPropertyValue", o1.getMyStringProperty());
    Assert.assertArrayEquals("o2.stringArrayField", new String[] {"a", "b", "c"}, o2.getStringArrayField());

    Assert.assertEquals("o2.mapProperty.key1", "key1Val", o2.getMapProperty().get("key1"));
    Assert.assertEquals("o2.mapProperty(key1.dot)", "key1dotVal", o2.getMapProperty().get("key1.dot"));
    Assert.assertEquals("o2.mapProperty(key2.dot)", "key2dotVal", o2.getMapProperty().get("key2.dot"));

  }

  public static class ValidationTestModule implements Module
  {
    @NotNull
    @Pattern(regexp=".*malhar.*", message="Value has to contain 'malhar'!")
    private String stringField1;

    @Min(2)
    private int intField1;

    @AssertTrue(message="stringField1 should end with intField1")
    private boolean isValidConfiguration() {
      return stringField1.endsWith(String.valueOf(intField1));
    }

    private String getterProperty2 = "";

    @NotNull
    public String getProperty2() {
      return getterProperty2;
    }

    public void setProperty2(String s) {
      // annotations need to be on the getter
      getterProperty2 = s;
    }

    private String[] stringArrayField;

    public String[] getStringArrayField() {
      return stringArrayField;
    }

    public void setStringArrayField(String[] stringArrayField) {
      this.stringArrayField = stringArrayField;
    }

    public class Nested {
      @NotNull
      private String property = "";

      public String getProperty() {
        return property;
      }

      public void setProperty(String property) {
        this.property = property;
      }

    }

    @Valid
    private final Nested nestedBean = new Nested();

    private String stringProperty2;

    public String getStringProperty2() {
      return stringProperty2;
    }

    public void setStringProperty2(String stringProperty2) {
      this.stringProperty2 = stringProperty2;
    }

    private Map<String, String> mapProperty = Maps.newHashMap();

    public Map<String, String> getMapProperty()
    {
      return mapProperty;
    }

    public void setMapProperty(Map<String, String> mapProperty)
    {
      this.mapProperty = mapProperty;
    }

    @Override public void populateDAG(DAG dag, Configuration conf)
    {

    }
  }

  static class AppWithModule implements StreamingApplication {
    @Override public void populateDAG(DAG dag, Configuration conf)
    {
      dag.addModule("m1", new TestModules.PiModule());
    }
  }

  @Test
  public void moduleAppTest() {
    Configuration conf = new Configuration(false);
    conf.set(StreamingApplication.DT_PREFIX + "module.m1.prop.size", "1000");
    LogicalPlanConfiguration pb = new LogicalPlanConfiguration(conf);

    LogicalPlan dag = new LogicalPlan();
    pb.prepareDAG(dag, new AppWithModule(), "TestApp");
    System.out.println("This is test");
  }

  static class AppModuleExpansion implements StreamingApplication {
    @Override public void populateDAG(DAG dag, Configuration conf)
    {
      RandGenModule randGenModule = dag.addModule("RandGenModule", RandGenModule.class);
      WrapperModule wrapperModule = dag.addModule("WrapperModule", WrapperModule.class);
      RandGen dummy = dag.addOperator("Dummy", RandGen.class);
    }
  }

  @Test
  public void dagExpansionTest() {
    Configuration conf = new Configuration(false);
    conf.set(StreamingApplication.DT_PREFIX + "module.WrapperModule.prop.size", "1000");

    LogicalPlanConfiguration lpc = new LogicalPlanConfiguration(conf);
    LogicalPlan dag = new LogicalPlan();
    lpc.prepareDAG(dag, new AppModuleExpansion(), "AppModuleExpansion");

    List<String> moduleNames = new ArrayList<>();
    for (LogicalPlan.ModuleMeta moduleMeta : dag.getAllModules()) {
      moduleNames.add(moduleMeta.getName());
    }
    Assert.assertTrue(moduleNames.contains("RandGenModule"));
    Assert.assertTrue(moduleNames.contains("WrapperModule"));
    Assert.assertTrue(moduleNames.contains("WrapperModule_PiModule"));

    List<String> operatorNames = new ArrayList<>();
    for (LogicalPlan.OperatorMeta operatorMeta : dag.getAllOperators()) {
      operatorNames.add(operatorMeta.getName());
    }
    Assert.assertTrue(operatorNames.contains("RandGenModule_RandGen"));
    Assert.assertTrue(operatorNames.contains("WrapperModule_PiModule_cal"));
    Assert.assertTrue(operatorNames.contains("WrapperModule_PiModule_gen"));
    Assert.assertTrue(operatorNames.contains("Dummy"));

    List<String> streamNames = new ArrayList<>();
    for (LogicalPlan.StreamMeta streamMeta : dag.getAllStreams()) {
      streamNames.add(streamMeta.getName());
    }
    Assert.assertTrue(streamNames.contains("WrapperModule_PiModule_s1"));

    Assert.assertTrue(dag.getOperatorMeta("RandGenModule_RandGen").getParentModuleName().equals("RandGenModule"));
    Assert.assertTrue(dag.getOperatorMeta("WrapperModule_PiModule_cal").getParentModuleName().equals("WrapperModule_PiModule"));
    Assert.assertTrue(dag.getOperatorMeta("WrapperModule_PiModule_gen").getParentModuleName().equals("WrapperModule_PiModule"));
    Assert.assertNull(dag.getOperatorMeta("Dummy").getParentModuleName());

    Assert.assertTrue(dag.getStream("WrapperModule_PiModule_s1").getParentModuleName().equals("WrapperModule_PiModule"));
  }
}
