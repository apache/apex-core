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
package com.datatorrent.stram.engine;

import java.io.Serializable;

import com.datatorrent.api.StringCodec;

/**
 *
 */
public class GenericOperatorProperty
{
  private String s;

  public GenericOperatorProperty(String s)
  {
    this.s = s;
  }

  // This is on purpose not to be a getter.
  public String obtainString()
  {
    return this.s;
  }

  public static class GenericOperatorPropertyStringCodec implements StringCodec<GenericOperatorProperty>, Serializable
  {
    private static final long serialVersionUID = 201403031223L;

    @Override
    public GenericOperatorProperty fromString(String string)
    {
      return new GenericOperatorProperty(string);
    }

    @Override
    public String toString(GenericOperatorProperty pojo)
    {
      return pojo.obtainString();
    }

  }
}
