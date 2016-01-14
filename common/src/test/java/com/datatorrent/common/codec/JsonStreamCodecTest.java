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
package com.datatorrent.common.codec;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.StringCodec;

public class JsonStreamCodecTest
{
  static class PojoClass
  {
    private InnerProperty x = new InnerProperty();
    private String s = "xyz";

    public InnerProperty getX()
    {
      return x;
    }

    public String getS()
    {
      return s;
    }
  }

  static class InnerProperty
  {

    public int getFoo()
    {
      throw new UnsupportedOperationException("test exception");
    }

    public String getBar()
    {
      return "hello world";
    }
  }

  static class PojoStringCodec implements StringCodec<InnerProperty>
  {

    @Override
    public InnerProperty fromString(String string)
    {
      return new InnerProperty();
    }

    @Override
    public String toString(InnerProperty pojo)
    {
      return "bar:" + pojo.getBar();
    }

  }

  @Test
  public void testJsonStreamCodec() throws Exception
  {
    Map<Class<?>, Class<? extends StringCodec<?>>> codecs = new HashMap<>();
    codecs.put(InnerProperty.class, PojoStringCodec.class);
    JsonStreamCodec jsc = new JsonStreamCodec(codecs);
    PojoClass obj = new PojoClass();
    String s = jsc.toByteArray(obj).stringValue();
    JSONObject json = new JSONObject(s);
    Assert.assertEquals("xyz", json.getString("s"));
    Assert.assertEquals("bar:hello world", json.getString("x"));
  }
}
