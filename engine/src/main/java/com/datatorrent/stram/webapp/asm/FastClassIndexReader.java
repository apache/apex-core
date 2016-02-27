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

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import org.apache.xbean.asm5.Opcodes;

/**
 *
 * An improvement for ASM class reader.
 * ASM class reader reads the whole class into structured data like ClassNode MethodNode
 * But to build a type graph we only need to know class name, parent class name, interfaces name and
 * whether it has a public non-arg default constructor (instantiable)
 * This class skip most parts that are not necessary and parse only the information above
 *
 * And also it use shared buffer to load classes which makes it faster in our performance test
 *
 * Overall it is 8-9x faster than ASM class reader
 *
 * Keep in mind it is NOT thread safe
 *
 *
 * @since 3.3.0
 */
public class FastClassIndexReader
{

  /**
   * The type of CONSTANT_Class constant pool items.
   */
  static final int CLASS = 7;

  /**
   * The type of CONSTANT_Fieldref constant pool items.
   */
  static final int FIELD = 9;

  /**
   * The type of CONSTANT_Methodref constant pool items.
   */
  static final int METH = 10;

  /**
   * The type of CONSTANT_InterfaceMethodref constant pool items.
   */
  static final int IMETH = 11;

  /**
   * The type of CONSTANT_String constant pool items.
   */
  static final int STR = 8;

  /**
   * The type of CONSTANT_Integer constant pool items.
   */
  static final int INT = 3;

  /**
   * The type of CONSTANT_Float constant pool items.
   */
  static final int FLOAT = 4;

  /**
   * The type of CONSTANT_Long constant pool items.
   */
  static final int LONG = 5;

  /**
   * The type of CONSTANT_Double constant pool items.
   */
  static final int DOUBLE = 6;

  /**
   * The type of CONSTANT_NameAndType constant pool items.
   */
  static final int NAME_TYPE = 12;

  /**
   * The type of CONSTANT_Utf8 constant pool items.
   */
  static final int UTF8 = 1;

  /**
   * The type of CONSTANT_MethodType constant pool items.
   */
  static final int MTYPE = 16;

  /**
   * The type of CONSTANT_MethodHandle constant pool items.
   */
  static final int HANDLE = 15;

  /**
   * The type of CONSTANT_InvokeDynamic constant pool items.
   */
  static final int INDY = 18;

  // shared buffer to hold the content of the file
  private static byte[] b = new byte[64 * 1024];

  private static int bSize = 0;

  private int[] items;

  private int header;

  private String name;

  private String superName;

  private String[] interfaces;

  // use this for byte array comparison which is faster than string comparison
  public static final byte[] DEFAULT_CONSTRUCTOR_NAME = new byte[]{0, 6, '<', 'i', 'n', 'i', 't', '>'};

  public static final byte[] DEFAULT_CONSTRUCTOR_DESC = new byte[]{0, 3, '(', ')', 'V'};

  private boolean isInstantiable = false;

  public FastClassIndexReader(final InputStream is) throws IOException
  {
    readIntoBuffer(is);

    readConstantPool();

    readIndex();
  }

  /**
   * Read class file content into shared buffer from input stream
   * Stream won't be closed
   * @param is
   * @throws IOException
   */
  private void readIntoBuffer(InputStream is) throws IOException
  {
    if (is == null) {
      throw new IOException("Class not found");
    }
    bSize = 0;
    while (true) {
      int n = is.read(b, bSize, b.length - bSize);
      if (n == -1) {
        break;
      }
      bSize += n;
      if (bSize >= b.length) {
        byte[] c = new byte[b.length << 2];
        System.arraycopy(b, 0, c, 0, b.length);
        b = c;
      }
    }
  }

  /**
   * read and index the constant pool section for getting class metadata later
   */
  private void readConstantPool()
  {
    // checks the class version
    if (readShort(6) > Opcodes.V1_8) {
      throw new IllegalArgumentException();
    }
    // parses the constant pool
    items = new int[readUnsignedShort(8)];
    int n = items.length;
    int index = 10;
    for (int i = 1; i < n; ++i) {
      items[i] = index + 1;
      int size;
      switch (b[index]) {
        case FIELD:
        case METH:
        case IMETH:
        case INT:
        case FLOAT:
        case NAME_TYPE:
        case INDY:
          size = 5;
          break;
        case LONG:
        case DOUBLE:
          size = 9;
          ++i;
          break;
        case UTF8:
          size = 3 + readUnsignedShort(index + 1);
          break;
        case HANDLE:
          size = 4;
          break;
        // case ClassWriter.CLASS:
        // case ClassWriter.STR:
        // case ClassWriter.MTYPE
        default:
          size = 3;
          break;
      }
      index += size;
    }
    // the class header information starts just after the constant pool
    header = index;
  }

  /**
   * read class metadata, class name, parent name, interfaces name, is instantiable(has public non-arg constructor)
   * or not
   * @throws UnsupportedEncodingException
   */
  private void readIndex() throws UnsupportedEncodingException
  {
    // reads the class declaration
    int u = header;

    int access = readUnsignedShort(u);
    isInstantiable = ASMUtil.isPublic(access) && !ASMUtil.isAbstract(access);

    name = readClass(u + 2);
    superName = readClass(u + 4);
    interfaces = new String[readUnsignedShort(u + 6)];
    u += 8;
    for (int i = 0; i < interfaces.length; ++i) {
      interfaces[i] = readClass(u);
      u += 2;
    }

    if (!isInstantiable) {
      return;
    }

    // reads the constructor

    // skip fields
    for (int i = readUnsignedShort(u); i > 0; --i) {
      for (int j = readUnsignedShort(u + 8); j > 0; --j) {
        u += 6 + readInt(u + 12);
      }
      u += 8;
    }
    u += 2;
    for (int i = readUnsignedShort(u); i > 0; --i) {
      if (isDefaultConstructor(u + 2)) {
        return;
      }
      for (int j = readUnsignedShort(u + 8); j > 0; --j) {
        u += 6 + readInt(u + 12);
      }
      u += 8;
    }
    isInstantiable = false;
  }

  private boolean isDefaultConstructor(int methodIndex) throws UnsupportedEncodingException
  {
    return arrayContains(b, items[readUnsignedShort(methodIndex + 2)], DEFAULT_CONSTRUCTOR_NAME)
      && arrayContains(b, items[readUnsignedShort(methodIndex + 4)], DEFAULT_CONSTRUCTOR_DESC)
      && ASMUtil.isPublic(readUnsignedShort(methodIndex));
  }

  private boolean arrayContains(byte[] bb, int i, byte[] subArray)
  {
    for (int l = 0; l < subArray.length; l++) {
      if (bb[i + l] != subArray[l]) {
        return false;
      }
    }
    return true;
  }

  public int readUnsignedShort(final int index)
  {
    return ((b[index] & 0xFF) << 8) | (b[index + 1] & 0xFF);
  }

  public short readShort(final int index)
  {
    return (short)(((b[index] & 0xFF) << 8) | (b[index + 1] & 0xFF));
  }

  public int readInt(final int index)
  {
    return ((b[index] & 0xFF) << 24) | ((b[index + 1] & 0xFF) << 16)
      | ((b[index + 2] & 0xFF) << 8) | (b[index + 3] & 0xFF);
  }

  public String readClass(final int index) throws UnsupportedEncodingException
  {
    // computes the start index of the CONSTANT_Class item in b
    // and reads the CONSTANT_Utf8 item designated by
    // the first two bytes of this CONSTANT_Class item
    return readUTF8(items[readUnsignedShort(index)]);
  }

  public String readUTF8(int index) throws UnsupportedEncodingException
  {
    int item = readUnsignedShort(index);
    if (index == 0 || item == 0) {
      return null;
    }
    index = items[item];
    return new String(b, index + 2, readUnsignedShort(index), "UTF-8");
  }

  public String getName()
  {
    return name;
  }

  public String getSuperName()
  {
    return superName;
  }

  public String[] getInterfaces()
  {
    return interfaces;
  }

  public boolean isInstantiable()
  {
    return isInstantiable;
  }
}
