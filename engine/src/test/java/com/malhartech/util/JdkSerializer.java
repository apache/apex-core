package com.malhartech.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
 * @param <T>
 *          type of serialized object
 */
public class JdkSerializer<T> extends Serializer<T> {

  @Override
  public void write(Kryo kryo, Output output, T t) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(t);
      oos.close();
      byte[] ba = baos.toByteArray();
      output.writeInt(ba.length);
      output.write(ba);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public T read(Kryo kryo, Input input, Class<T> type) {
    try {
      int length = input.readInt();
      byte[] ba = new byte[length];
      input.readBytes(ba);
      ByteArrayInputStream bais = new ByteArrayInputStream(ba);
      ObjectInputStream ois = new ObjectInputStream(bais);
      @SuppressWarnings("unchecked")
      T t = (T) ois.readObject();
      return t;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}