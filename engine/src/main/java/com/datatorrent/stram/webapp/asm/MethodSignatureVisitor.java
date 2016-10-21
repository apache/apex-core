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
package com.datatorrent.stram.webapp.asm;

import java.util.LinkedList;
import java.util.List;

import org.apache.xbean.asm5.signature.SignatureVisitor;

/**
 * A method signature parser
 *
 * @since 2.1.0
 */
public class MethodSignatureVisitor extends BaseSignatureVisitor
{

  // There is at most 1 parameter for setter and getter method
  private List<Type> parameters = new LinkedList<>();

  private Type returnType;

  private List<Type> exceptionType = new LinkedList<>();

  public static final int VISIT_PARAM = 1;

  public static final int VISIT_RETURN = 2;

  public static final int VISIT_EXCEPTION = 3;


  @Override
  public SignatureVisitor visitExceptionType()
  {
    if (stage == VISIT_RETURN && !visitingStack.isEmpty()) {
      returnType = visitingStack.pop();
    }
    if (stage == VISIT_EXCEPTION && !visitingStack.isEmpty()) {
      exceptionType.add(visitingStack.pop());
    }
    stage = VISIT_EXCEPTION;

    return this;
  }

  @Override
  public SignatureVisitor visitParameterType()
  {
    if (stage == VISIT_FORMAL_TYPE) {
      stage = VISIT_PARAM;
      if (!visitingStack.isEmpty()) {
        visitingStack.pop();
      }
      return this;
    }
    stage = VISIT_PARAM;
    if (!visitingStack.isEmpty()) {
      parameters.add(visitingStack.pop());
    }
    return this;
  }


  @Override
  public SignatureVisitor visitReturnType()
  {

    while (!visitingStack.isEmpty()) {
      parameters.add(visitingStack.pop());
    }
    stage = VISIT_RETURN;
    return this;
  }


  public Type getReturnType()
  {
    if (returnType == null && !visitingStack.isEmpty()) {
      returnType = visitingStack.pop();
    }
    return returnType;
  }

  public List<Type> getParameters()
  {
    return parameters;
  }

}
