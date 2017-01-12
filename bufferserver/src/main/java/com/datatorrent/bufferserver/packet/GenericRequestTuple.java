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
package com.datatorrent.bufferserver.packet;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.netlet.util.VarInt;

/**
 * <p>GenericRequestTuple class.</p>
 *
 * @since 0.3.2
 */
public class GenericRequestTuple extends RequestTuple
{
  public static final String EMPTY_STRING = new String();
  public String version;
  protected String identifier;
  protected int baseSeconds;
  protected int windowId;

  public GenericRequestTuple(byte[] buffer, int offset, int length)
  {
    super(buffer, offset, length);
  }

  @Override
  public MessageType getType()
  {
    return null;
  }

  @Override
  public boolean isValid()
  {
    return valid;
  }

  @Override
  protected void parse()
  {
    parsed = true;

    try {
      /*
       * read the version.
       */
      int idlen = readVarInt();
      if (idlen > 0) {
        version = new String(buffer, offset, idlen);
        offset += idlen;
      } else if (idlen == 0) {
        version = EMPTY_STRING;
      } else {
        return;
      }
      /*
       * read the identifier.
       */
      idlen = readVarInt();
      if (idlen > 0) {
        identifier = new String(buffer, offset, idlen);
        offset += idlen;
      } else if (idlen == 0) {
        identifier = EMPTY_STRING;
      } else {
        return;
      }

      baseSeconds = readVarInt();

      windowId = readVarInt();

      valid = true;
    } catch (NumberFormatException nfe) {
      logger.warn("Unparseable Tuple", nfe);
    }
  }

  @Override
  public int getWindowId()
  {
    return windowId;
  }

  @Override
  public int getBaseSeconds()
  {
    return baseSeconds;
  }

  @Override
  public String getVersion()
  {
    return version;
  }

  @Override
  public String getIdentifier()
  {
    return identifier;
  }

  public static byte[] getSerializedRequest(String version, String identifier, long startingWindowId, byte type)
  {
    byte[] array = new byte[4096];
    int offset = 0;

    /* write the type */
    array[offset++] = type;

    /* write the version */
    if (version == null) {
      version = CLASSIC_VERSION;
    }
    offset = writeString(version, array, offset);

    /* write the identifer */
    offset = writeString(identifier, array, offset);

    /* write the baseSeconds */
    int baseSeconds = (int)(startingWindowId >> 32);
    offset = VarInt.write(baseSeconds, array, offset);

    /* write the windowId */
    int windowId = (int)startingWindowId;
    offset = VarInt.write(windowId, array, offset);

    return Arrays.copyOfRange(array, 0, offset);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" + "version=" + version + ", identifier=" + identifier + ", windowId=" +
        Codec.getStringWindowId((long)baseSeconds << 32 | windowId) + '}';
  }

  private static final Logger logger = LoggerFactory.getLogger(GenericRequestTuple.class);
}
