/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.util;

import com.malhartech.bufferserver.Buffer.Message;
import com.malhartech.bufferserver.Buffer.Message.MessageType;
import com.malhartech.bufferserver.Buffer.ResetWindow;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class TupleFactory
{
  public static Message getResetTuple(int baseSeconds, int intervalMillis)
  {
    ResetWindow.Builder rwb = ResetWindow.newBuilder();
    rwb.setBaseSeconds(baseSeconds);
    rwb.setWidth(intervalMillis);

    Message.Builder db = Message.newBuilder();
    db.setType(MessageType.RESET_WINDOW);
    db.setResetWindow(rwb);

    return db.build();
  }

  private TupleFactory()
  {
  }
}
