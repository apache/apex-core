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
package com.datatorrent.stram.api;

import java.util.Map;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * <p>AppDataSource class.</p>
 *
 * @since 3.2.0
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class AppDataSource
{

  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  public static class QueryInfo
  {
    public String operatorName;
    public String url;
    public String topic;
  }

  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  public static class ResultInfo
  {
    public String operatorName;
    public String url;
    public String topic;
    public boolean appendQIDToTopic;
  }

  public enum Type
  {
    DAG("dag"), APP_STATS("appStats"), OPERATOR_STATS("operatorStats"), CUSTOM_METRICS("metrics");

    private final String name;

    private Type(String name)
    {
      this.name = name;
    }

    @Override
    public String toString()
    {
      return name;
    }
  }

  private Type type;
  private String operatorName;
  private String portName;
  private final QueryInfo query = new QueryInfo();
  private final ResultInfo result = new ResultInfo();
  private Map<String, String> schemaKeys;

  public Type getType()
  {
    return type;
  }

  public String getName()
  {
    return operatorName + "." + portName;
  }

  public String getOperatorName()
  {
    return operatorName;
  }

  public String getPortName()
  {
    return portName;
  }

  public QueryInfo getQuery()
  {
    return query;
  }

  public ResultInfo getResult()
  {
    return result;
  }

  public Map<String, String> getSchemaKeys()
  {
    return schemaKeys;
  }

  public void setType(Type type)
  {
    this.type = type;
  }

  public void setSchemaKeys(Map<String, String> schemaKeys)
  {
    this.schemaKeys = schemaKeys;
  }

  public void setOperatorName(String operatorName)
  {
    this.operatorName = operatorName;
  }

  public void setPortName(String portName)
  {
    this.portName = portName;
  }

  public void setQueryOperatorName(String name)
  {
    this.query.operatorName = name;
  }

  public void setQueryUrl(String queryUrl)
  {
    this.query.url = queryUrl;
  }

  public void setQueryTopic(String queryTopic)
  {
    this.query.topic = queryTopic;
  }

  public void setResultOperatorName(String name)
  {
    this.result.operatorName = name;
  }

  public void setResultUrl(String resultUrl)
  {
    this.result.url = resultUrl;
  }

  public void setResultTopic(String resultTopic)
  {
    this.result.topic = resultTopic;
  }

  public void setResultAppendQIDTopic(boolean b)
  {
    this.result.appendQIDToTopic = b;
  }
}
