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

import java.util.Collection;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.classification.InterfaceStability;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * This is an implementation of {@link Serde} which serializes and deserializes lists.
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public class CollectionSerde<T, CollectionT extends Collection<T>> implements Serde<CollectionT>
{
  @NotNull
  private Serde<T> serde;

  @NotNull
  private Class<? extends CollectionT> collectionClass;

  private CollectionSerde()
  {
    // for Kryo
  }

  /**
   * Creates a {@link CollectionSerde}.
   * @param serde The {@link Serde} that is used to serialize and deserialize each element of a list.
   */
  public CollectionSerde(@NotNull Serde<T> serde, @NotNull Class<? extends CollectionT> collectionClass /*Class<? extends C1> collectionClass*/ )
  {
    this.serde = Preconditions.checkNotNull(serde);
    this.collectionClass = Preconditions.checkNotNull(collectionClass);
  }

  @Override
  public void serialize(CollectionT objects, Output output)
  {
    if (objects.size() == 0) {
      return;
    }
    output.writeInt(objects.size(), true);
    Serde<T> serializer = getItemSerde();
    for (T object : objects) {
      serializer.serialize(object, output);
    }
  }

  @Override
  public CollectionT deserialize(Input input)
  {
    int numElements = input.readInt(true);

    try {
      CollectionT collection = collectionClass.newInstance();

      for (int index = 0; index < numElements; index++) {
        T object = serde.deserialize(input);
        collection.add(object);
      }

      return collection;
    } catch (Exception ex) {
      throw Throwables.propagate(ex);
    }
  }

  protected Serde<T> getItemSerde()
  {
    return serde;
  }
}
