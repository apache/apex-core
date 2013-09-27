/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.engine;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.datatorrent.api.Operator.ProcessingMode;

import com.datatorrent.stram.engine.ProcessingModeTests.CollectorOperator;

/**
 // make a determination of the commented out assertTrues below as to what
 // should happen. Right now it's left undecided since we do not know the
 // implementation of recovery checkpoint in the stram.
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
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
    Assert.assertEquals("Generated Outputs", maxTuples, CollectorOperator.collection.size());
    //Assert.assertTrue("No Duplicates", CollectorOperator.duplicates.isEmpty());
  }

  @Ignore // Pramod and Thomas are working on it, they should enable it when it passes.
  @Test
  @Override
  public void testLinearOperatorRecovery() throws Exception
  {
    super.testLinearOperatorRecovery();
    Assert.assertEquals("Generated Outputs", maxTuples, CollectorOperator.collection.size());
    //Assert.assertTrue("No Duplicates", CollectorOperator.duplicates.isEmpty());
  }

  @Test
  @Override
  public void testLinearInlineOperatorsRecovery() throws Exception
  {
    super.testLinearInlineOperatorsRecovery();
    Assert.assertEquals("Generated Outputs", maxTuples, CollectorOperator.collection.size());
    //Assert.assertTrue("No Duplicates", CollectorOperator.duplicates.isEmpty());
  }
}
