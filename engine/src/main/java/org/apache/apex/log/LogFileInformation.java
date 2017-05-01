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
package org.apache.apex.log;

import com.datatorrent.stram.util.AbstractWritableAdapter;

/**
 * @since 3.6.0
 */
public class LogFileInformation extends AbstractWritableAdapter
{

  private static final long serialVersionUID = 1L;
  public String fileName;
  public long fileOffset;

  public LogFileInformation()
  {
  }

  public LogFileInformation(String fileName, long fileOffset)
  {
    this.fileName = fileName;
    this.fileOffset = fileOffset;
  }

  @Override
  public String toString()
  {
    return "LogFileInformation [fileName=" + fileName + ", fileOffset=" + fileOffset + "]";
  }
}
