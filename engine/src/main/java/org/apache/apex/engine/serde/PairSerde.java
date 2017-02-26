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
package org.apache.apex.engine.serde;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceStability;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;

/**
 * This is an implementation of {@link Serde} which serializes and deserializes pairs.
 */
@InterfaceStability.Evolving
public class PairSerde<T1, T2> implements Serde<Pair<T1, T2>>
{
  @NotNull
  private Serde<T1> serde1;
  @NotNull
  private Serde<T2> serde2;

  private PairSerde()
  {
    // for Kryo
  }

  /**
   * Creates a {@link PairSerde}.
   * @param serde1 The {@link Serde} that is used to serialize and deserialize first element of a pair
   * @param serde2 The {@link Serde} that is used to serialize and deserialize second element of a pair
   */
  public PairSerde(@NotNull Serde<T1> serde1, @NotNull Serde<T2> serde2)
  {
    this.serde1 = Preconditions.checkNotNull(serde1);
    this.serde2 = Preconditions.checkNotNull(serde2);
  }

  @Override
  public void serialize(Pair<T1, T2> pair, Output output)
  {
    serde1.serialize(pair.getLeft(), output);
    serde2.serialize(pair.getRight(), output);
  }

  @Override
  public Pair<T1, T2> deserialize(Input input)
  {
    T1 first = serde1.deserialize(input);
    T2 second = serde2.deserialize(input);
    return new ImmutablePair<>(first, second);
  }

}
