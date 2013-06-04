package com.malhartech.api.codec;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.malhartech.api.annotation.ShipContainingJars;

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
 * @param <T> type of serialized object
 */
@ShipContainingJars(classes = {com.esotericsoftware.kryo.Kryo.class})
public class KryoJdkSerializer<T> extends Serializer<T>
{
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

  @Override
  @SuppressWarnings("unchecked")
  public T read(Kryo kryo, Input input, Class<T> type)
  {
    try {
      ObjectInputStream ois = new ObjectInputStream(input);
      return (T)ois.readObject();
    }
    catch (ClassNotFoundException ex) {
      throw new RuntimeException(ex);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

}