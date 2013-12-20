/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import com.datatorrent.stram.api.StramEvent;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.beanutils.BeanUtils;

/**
 * <p>EventRecorder interface.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.4
 */
public interface EventRecorder
{
  public void recordEventAsync(StramEvent event);

}
