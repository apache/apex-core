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

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.Operator.ProcessingMode;


/**
 *
 */
public class ExactlyOnceTest extends ProcessingModeTests
{
  public ExactlyOnceTest()
  {
    super(ProcessingMode.EXACTLY_ONCE);
  }

  @Test
  @Override
  public void testLinearInputOperatorRecovery() throws Exception
  {
    super.testLinearInputOperatorRecovery();
    Assert.assertTrue("Generated Outputs", maxTuples >= CollectorOperator.collection.size());
    long idx = 0L;
    for (long tuple : RecoverableInputOperator.emittedTuples) {
      Assert.assertEquals("Emitted tuple: ", tuple, idx++);
    }
    Assert.assertTrue("No Duplicates", CollectorOperator.duplicates.isEmpty());
  }

  @Test
  @Override
  public void testLinearOperatorRecovery() throws Exception
  {
    super.testLinearOperatorRecovery();
    Assert.assertEquals("Generated Outputs", maxTuples, CollectorOperator.collection.size());
    Assert.assertTrue("No Duplicates", CollectorOperator.duplicates.isEmpty());
  }

  //@Test
  @Override
  public void testLinearInlineOperatorsRecovery() throws Exception
  {
    super.testLinearInlineOperatorsRecovery();
    Assert.assertEquals("Generated Outputs", maxTuples, CollectorOperator.collection.size());
    Assert.assertTrue("No Duplicates", CollectorOperator.duplicates.isEmpty());
  }
}
