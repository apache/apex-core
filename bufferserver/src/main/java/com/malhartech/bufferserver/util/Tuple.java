/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.util;

import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.Buffer.Data.DataType;
import com.malhartech.bufferserver.Buffer.ResetWindow;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Tuple
{
  public static Data getResetTuple(int baseSeconds, int intervalMillis)
  {
    Data.Builder db = Data.newBuilder();
    db.setType(DataType.RESET_WINDOW);
    db.setWindowId(baseSeconds);

    ResetWindow.Builder rwb = ResetWindow.newBuilder();
    rwb.setWidth(intervalMillis);
    db.setResetWindow(rwb);

    return db.build();
  }
}
