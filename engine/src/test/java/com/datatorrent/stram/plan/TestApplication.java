package com.datatorrent.stram.plan;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;


public class TestApplication implements StreamingApplication {
  Integer testprop1;
  Integer testprop2;
  Integer testprop3;
  TestInnerClass inncls;
  public TestApplication() {
    inncls=new TestInnerClass();
  }

  public Integer getTestprop1() {
    return testprop1;
  }

  public void setTestprop1(Integer testprop1) {
    this.testprop1 = testprop1;
  }

  public Integer getTestprop2() {
    return testprop2;
  }

  public void setTestprop2(Integer testprop2) {
    this.testprop2 = testprop2;
  }

  public Integer getTestprop3() {
    return testprop3;
  }

  public void setTestprop3(Integer testprop3) {
    this.testprop3 = testprop3;
  }

  public TestInnerClass getInncls() {
    return inncls;
  }

  public void setInncls(TestInnerClass inncls) {
    this.inncls = inncls;
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf) {

  }
  public class TestInnerClass{
    Integer a;

    public Integer getA() {
      return a;
    }

    public void setA(Integer a) {
      this.a = a;
    }
  }
}
