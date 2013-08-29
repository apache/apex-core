/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.testng.Assert.*;
import org.testng.annotations.Test;

import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.bufferserver.util.SerializedData;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class CodecTest
{
  @Test
  public void testSomeMethod()
  {
    byte buffer[] = new byte[10];
    int value = 127;
    Codec.writeRawVarint32(value, buffer, 0);

    SerializedData sd = new SerializedData(buffer, 0, 0);
    Codec.readRawVarInt32(sd);
    assertEquals(sd.size - sd.dataOffset, value);

    Codec.writeRawVarint32(value, buffer, 0, 5);
    sd.size = 0;
    sd.dataOffset = 0;
    Codec.readRawVarInt32(sd);
    assertEquals(sd.size - sd.dataOffset, value);
  }

  private static final Logger logger = LoggerFactory.getLogger(CodecTest.class);
}
