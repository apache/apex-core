/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.stram.util;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class OperatorBeanUtils
{
  private static final int MAX_PROPERTY_LEVELS = 5;

  public static JSONArray getClassProperties(Class<?> clazz, int level) throws IntrospectionException, JSONException
  {
    JSONArray arr = new JSONArray();
    for (PropertyDescriptor pd: Introspector.getBeanInfo(clazz).getPropertyDescriptors()) {
      if (!pd.getName().equals("class") && (!(pd.getName().equals("up") && pd.getPropertyType().equals(com.datatorrent.api.Context.class)))) {
        JSONObject propertyObj = new JSONObject();
        propertyObj.put("name", pd.getName());
        propertyObj.put("canGet", pd.getReadMethod() != null);
        propertyObj.put("canSet", pd.getWriteMethod() != null);
        propertyObj.put("description", pd.getShortDescription());
        Class<?> propertyType = pd.getPropertyType();
        propertyObj.put("type", propertyType.getName());
        if (!propertyType.isPrimitive() && !propertyType.isEnum() && !propertyType.isArray() && !propertyType.getName().startsWith("java.lang") && level < MAX_PROPERTY_LEVELS) {
          propertyObj.put("properties", getClassProperties(propertyType, level + 1));
        }
        arr.put(propertyObj);
      }
    }
    return arr;
  }

}
