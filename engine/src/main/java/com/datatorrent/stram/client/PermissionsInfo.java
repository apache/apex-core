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
package com.datatorrent.stram.client;

import java.util.Set;
import java.util.TreeSet;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * <p>PermissionsInfo class.</p>
 *
 * @since 2.0.0
 */
public class PermissionsInfo
{

  private final Set<String> readOnlyRoles = new TreeSet<>();
  private final Set<String> readOnlyUsers = new TreeSet<>();
  private final Set<String> readWriteRoles = new TreeSet<>();
  private final Set<String> readWriteUsers = new TreeSet<>();
  private boolean readOnlyEveryone = false;
  private boolean readWriteEveryone = false;

  public PermissionsInfo()
  {
  }

  public PermissionsInfo(JSONObject json) throws JSONException
  {
    JSONObject readOnly = json.optJSONObject("readOnly");
    JSONObject readWrite = json.optJSONObject("readWrite");
    if (readOnly != null) {
      JSONArray users = readOnly.optJSONArray("users");
      if (users != null) {
        for (int i = 0; i < users.length(); i++) {
          readOnlyUsers.add(users.getString(i));
        }
      }
      JSONArray roles = readOnly.optJSONArray("roles");
      if (roles != null) {
        for (int i = 0; i < roles.length(); i++) {
          readOnlyRoles.add(roles.getString(i));
        }
      }
      readOnlyEveryone = readOnly.optBoolean("everyone", false);
    }
    if (readWrite != null) {
      JSONArray users = readWrite.optJSONArray("users");
      if (users != null) {
        for (int i = 0; i < users.length(); i++) {
          readWriteUsers.add(users.getString(i));
        }
      }
      JSONArray roles = readWrite.optJSONArray("roles");
      if (roles != null) {
        for (int i = 0; i < roles.length(); i++) {
          readWriteRoles.add(roles.getString(i));
        }
      }
      readWriteEveryone = readWrite.optBoolean("everyone", false);
    }
  }

  public void addReadOnlyRole(String role)
  {
    readOnlyRoles.add(role);
  }

  public void removeReadOnlyRole(String role)
  {
    readOnlyRoles.remove(role);
  }

  public void addReadOnlyUser(String user)
  {
    readOnlyUsers.add(user);
  }

  public void removeReadOnlyUser(String user)
  {
    readOnlyUsers.remove(user);
  }

  public void setReadOnlyEveryone(boolean readOnlyEveryone)
  {
    this.readOnlyEveryone = readOnlyEveryone;
  }

  public void addReadWriteRole(String role)
  {
    readWriteRoles.add(role);
  }

  public void removeReadWriteRole(String role)
  {
    readWriteRoles.remove(role);
  }

  public void addReadWriteUser(String user)
  {
    readWriteRoles.add(user);
  }

  public void removeReadWriteUser(String user)
  {
    readWriteUsers.remove(user);
  }

  public void setReadWriteEveryone(boolean readWriteEveryone)
  {
    this.readWriteEveryone = readWriteEveryone;
  }

  public boolean canRead(String userName, Set<String> roles)
  {
    if (canWrite(userName, roles)) {
      return true;
    }
    if (readOnlyEveryone) {
      return true;
    }
    if (readOnlyUsers.contains(userName)) {
      return true;
    }
    for (String role : roles) {
      if (readOnlyRoles.contains(role)) {
        return true;
      }
    }
    return false;
  }

  public boolean canWrite(String userName, Set<String> roles)
  {
    if (readWriteEveryone) {
      return true;
    }
    if (readWriteUsers.contains(userName)) {
      return true;
    }
    for (String role : roles) {
      if (readWriteRoles.contains(role)) {
        return true;
      }
    }
    return false;
  }

  public JSONObject toJSONObject()
  {
    JSONObject result = new JSONObject();
    JSONObject readOnly = new JSONObject();
    JSONObject readWrite = new JSONObject();
    try {
      readOnly.put("users", new JSONArray(readOnlyUsers));
      readOnly.put("roles", new JSONArray(readOnlyRoles));
      readOnly.put("everyone", readOnlyEveryone);
      readWrite.put("users", new JSONArray(readWriteUsers));
      readWrite.put("roles", new JSONArray(readWriteRoles));
      readWrite.put("everyone", readWriteEveryone);
      result.put("readOnly", readOnly);
      result.put("readWrite", readWrite);
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
    return result;
  }
}
