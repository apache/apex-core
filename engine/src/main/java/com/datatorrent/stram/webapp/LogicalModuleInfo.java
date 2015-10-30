package com.datatorrent.stram.webapp;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "Module")
@XmlAccessorType(XmlAccessType.FIELD)
public class LogicalModuleInfo
{
  public String name;
  public String className;
  public List<LogicalModuleInfo> modules = new ArrayList<LogicalModuleInfo>();
  public List<LogicalOperatorInfo> operators = new ArrayList<LogicalOperatorInfo>();
  public List<StreamInfo> streams = new ArrayList<StreamInfo>();
}
