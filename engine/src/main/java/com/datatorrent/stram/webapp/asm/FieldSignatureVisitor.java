/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.stram.webapp.asm;


import org.apache.xbean.asm5.signature.SignatureVisitor;

public class FieldSignatureVisitor extends BaseSignatureVisitor{

  private Type fieldType;
  
  @Override
  public SignatureVisitor visitExceptionType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SignatureVisitor visitParameterType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SignatureVisitor visitReturnType() {
    throw new UnsupportedOperationException();
  }

  public Type getFieldType() {
    if (!visitingStack.isEmpty()) {
      fieldType = visitingStack.pop();
      ;
      visitingStack.push(fieldType);
      return fieldType;
    } else {
      return null;
    }
  }
}
