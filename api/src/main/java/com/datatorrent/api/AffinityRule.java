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
package com.datatorrent.api;

import java.io.Serializable;
import java.util.List;

import com.datatorrent.api.DAG.Locality;

/**
 * Affinity rule specifies constraints for physical deployment of operator
 * containers. There are two types of rules that can be specified: Affinity and
 * Anti-affinity. Each rule contains list of operators or pair of 2 operators or
 * a regex that should match at least 2 operators. Based on the type of rule,
 * affinity or anti-affinity, the operators will be deployed together or away
 * from each other. The locality indicates the level at which the rule should be
 * applied. E.g. CONTAINER_LOCAL affinity would indicate operators Should be
 * allocated within same container NODE_LOCAL anti-affinity indicates that the
 * operators should not be allocated on the same node. The rule can be either
 * strict or relaxed.
 *
 */
public class AffinityRule implements Serializable
{
  @Override
  public String toString()
  {
    return "AffinityRule {operatorsList=" + operatorsList + ", operatorRegex=" + operatorRegex + ", operators="
        + operators + ", locality=" + locality + ", type=" + type + ", relaxLocality=" + relaxLocality + "}";
  }

  private static final long serialVersionUID = 107131504929875386L;

  /**
   * Pair of operator names to specify affinity rule
   * The order of operators is not considered in this class
   * i.e. OperatorPair("O1", "O2") is equal to OperatorPair("O2", "O1")
   */
  public static class OperatorPair implements Serializable
  {
    @Override
    public String toString()
    {
      return "OperatorPair (first=" + first + ", second=" + second + ")";
    }

    private static final long serialVersionUID = 4636942499106381268L;
    public String first;
    public String second;

    public OperatorPair(String first, String second)
    {
      this.first = first;
      this.second = second;
    }

    public OperatorPair()
    {
    }

    @Override
    public boolean equals(Object obj)
    {
      if (obj instanceof OperatorPair) {
        OperatorPair pairObj = (OperatorPair)obj;
        // The pair objects are equal if same 2 operators are present in both pairs
        // Order does not matter
        return ((this.first.equals(pairObj.first)) && (this.second.equals(pairObj.second)))
            || (this.first.equals(pairObj.second) && this.second.equals(pairObj.first));
      }
      return super.equals(obj);
    }

    @Override
    public int hashCode()
    {
      return this.first.hashCode() + this.second.hashCode();
    }
  }

  /**
   * Type of affinity rule setting affects how operators are scheduled for
   * deployment by the platform.
   */
  public static enum Type
  {
    /**
     * AFFINITY indicates that operators in the rule should be deployed within
     * locality specified in the rule
     */
    AFFINITY,
    /**
     * ANTI_AFFINITY indicates that operators in the rule should NOT deployed
     * within same locality as specified in rule
     */
    ANTI_AFFINITY
  }

  private List<String> operatorsList;
  private String operatorRegex;
  private OperatorPair operators;
  private Locality locality;
  private Type type;
  private boolean relaxLocality;

  public AffinityRule()
  {
  }

  public AffinityRule(Type type, Locality locality, boolean relaxLocality)
  {
    this.type = type;
    this.locality = locality;
    this.setRelaxLocality(relaxLocality);
  }

  public AffinityRule(Type type, OperatorPair operatorsPair, Locality locality, boolean relaxLocality)
  {
    this(type, locality, relaxLocality);
    this.operators = operatorsPair;
  }

  public AffinityRule(Type type, List<String> operatorsList, Locality locality, boolean relaxLocality)
  {
    this(type, locality, relaxLocality);
    this.operatorsList = operatorsList;
  }

  public AffinityRule(Type type, String operatorRegex, Locality locality, boolean relaxLocality)
  {
    this(type, locality, relaxLocality);
    this.operatorRegex = operatorRegex;
  }

  public OperatorPair getOperators()
  {
    return operators;
  }

  public void setOperators(OperatorPair operators)
  {
    this.operators = operators;
  }

  public Locality getLocality()
  {
    return locality;
  }

  public void setLocality(Locality locality)
  {
    this.locality = locality;
  }

  public Type getType()
  {
    return type;
  }

  public void setType(Type type)
  {
    this.type = type;
  }

  public boolean isRelaxLocality()
  {
    return relaxLocality;
  }

  public void setRelaxLocality(boolean relaxLocality)
  {
    this.relaxLocality = relaxLocality;
  }

  public List<String> getOperatorsList()
  {
    return operatorsList;
  }

  public void setOperatorsList(List<String> operatorsList)
  {
    this.operatorsList = operatorsList;
  }

  public String getOperatorRegex()
  {
    return operatorRegex;
  }

  public void setOperatorRegex(String operatorRegex)
  {
    this.operatorRegex = operatorRegex;
  }

}
