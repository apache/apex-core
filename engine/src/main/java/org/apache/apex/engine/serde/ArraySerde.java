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

import java.lang.reflect.Array;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;

public class ArraySerde<T> implements Serde<T[]>
{
  private Serde<T> itemSerde;
  private Class<T> itemType;

  private ArraySerde()
  {
  }

  /**
   * Serializer and Deserializer need different constructor, so use static factory method to wrap.
   * The ArraySerde returned by newSerializer can only used for serialization
   */
  public static <T> ArraySerde<T> newSerializer(Serde<T> itemSerde)
  {
    return new ArraySerde<T>(Preconditions.checkNotNull(itemSerde));
  }

  public static <T> ArraySerde<T> newSerde(Serde<T> itemSerde, Class<T> itemType)
  {
    return new ArraySerde<T>(Preconditions.checkNotNull(itemSerde), Preconditions.checkNotNull(itemType));
  }

  private ArraySerde(Serde<T> itemSerde)
  {
    this.itemSerde = itemSerde;
  }

  private ArraySerde(Serde<T> itemSerde, Class<T> itemType)
  {
    this.itemSerde = itemSerde;
    this.itemType = itemType;
  }

  @Override
  public void serialize(T[] objects, Output output)
  {
    if (objects.length == 0) {
      return;
    }
    output.writeInt(objects.length, true);
    Serde<T> serializer = getItemSerde();
    for (T object : objects) {
      serializer.serialize(object, output);
    }
  }

  protected Serde<T> getItemSerde()
  {
    return itemSerde;
  }

  @Override
  public T[] deserialize(Input input)
  {
    int numOfElements = input.readInt(true);

    T[] array = createObjectArray(numOfElements);

    for (int index = 0; index < numOfElements; ++index) {
      array[index] = getItemSerde().deserialize(input);
    }
    return array;
  }

  @SuppressWarnings("unchecked")
  protected T[] createObjectArray(int length)
  {
    return (T[])Array.newInstance(itemType, length);
  }
}
