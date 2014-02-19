/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.api.codec;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Kryo serializer implementation that can be annotated to operator or tuple
 * classes to have them serialized using JDK serialization instead default Kryo
 * serialization.
 * <p>
 * This implementation is not optimized for speed, JDK serialization is slower
 * than Kryo and should be used as the last resort if you know that the slowness
 * of it is not going to prevent you from operating your application in
 * realtime.
 * <p>
 * Should only be used for classes that cannot be handled by the default Kryo
 * serializer, for example:
 * <ul>
 * <li>Third party classes that don't have a default constructor (Kryo requires
 * at least a private no-argument constructor)
 * <li>Non-static inner classes (constructors have implicit outer class
 * reference)
 * <li>Classes with non-transient members that are not supported by Kryo
 * </ul>
 * JDK deserialization won't re-initialize transient members. Port objects on
 * operators that have to be transient when using Kryo have to be non-transient /
 * serializable, when using JDK serialization.
 *
 * This class is deprecated since there is a better implementation via
 * com.esotericsoftware.kryo.serializers.JavaSerializer in the Kryo library.
 *
 * @param <T> type of serialized object
 * @since 0.3.2
 * @deprecated Please use com.esotericsoftware.kryo.serializers.JavaSerializer instead.
 */
@Deprecated
public class KryoJdkSerializer<T> extends Serializer<T>
{
  /** {@inheritDoc} */
  @Override
  public void write(Kryo kryo, Output output, T t)
  {
    try {
      ObjectOutputStream oos = new ObjectOutputStream(output);
      oos.writeObject(t);
      oos.flush();
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("unchecked")
  public T read(Kryo kryo, Input input, Class<T> type)
  {
    try {
      ObjectInputStream ois = new ObjectInputStream(input);
      return (T)ois.readObject();
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

}
