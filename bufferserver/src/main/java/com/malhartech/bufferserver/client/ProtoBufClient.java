/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.client;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.malhartech.bufferserver.Buffer;
import com.malhartech.bufferserver.Buffer.Message;
import java.io.IOException;
import java.util.Arrays;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class ProtoBufClient extends VarIntLengthPrependerClient
{
  static final ExtensionRegistry registry = ExtensionRegistry.newInstance();

  static {
    Buffer.registerAllExtensions(registry);
  }

  @Override
  public void onMessage(byte[] buffer, int offset, int size)
  {
    try {
      onMessage(Message.newBuilder().mergeFrom(buffer, offset, size, registry).build());
    }
    catch (InvalidProtocolBufferException ex) {
      logger.debug("parse error", ex);
      throw new RuntimeException(ex);
    }
    catch (IOException ex) {
      logger.debug("IO exception", ex);
      throw new RuntimeException(ex);
    }
  }

  public abstract void onMessage(Message msg);

  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ProtoBufClient.class);
}
