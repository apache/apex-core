/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.stram.util;

import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.stram.webapp.OperatorDiscoverer;
import java.beans.*;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Comparator;
import org.codehaus.jettison.json.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * OperatorUtils class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.9.4
 */
public class OperatorUtils
{
  private static final int MAX_PROPERTY_LEVELS = 5;
  private static final Logger LOG = LoggerFactory.getLogger(OperatorUtils.class);

  public static JSONObject describeOperator(Class<? extends Operator> clazz) throws IntrospectionException
  {
    if (OperatorDiscoverer.isInstantiableOperatorClass(clazz)) {
      JSONObject response = new JSONObject();
      JSONArray inputPorts = new JSONArray();
      JSONArray outputPorts = new JSONArray();
      JSONArray properties = null;

      properties = OperatorUtils.getClassProperties(clazz);

      Field[] fields = clazz.getFields();
      Arrays.sort(fields, new Comparator<Field>()
          {
            @Override
            public int compare(Field a, Field b)
            {
              return a.getName().compareTo(b.getName());
            }

      });
      try {
        for (Field field : fields) {
          InputPortFieldAnnotation inputAnnotation = field.getAnnotation(InputPortFieldAnnotation.class);
          if (inputAnnotation != null) {
            JSONObject inputPort = new JSONObject();
            inputPort.put("name", inputAnnotation.name());
            inputPort.put("optional", inputAnnotation.optional());
            inputPorts.put(inputPort);
            continue;
          }
          else if (InputPort.class.isAssignableFrom(field.getType())) {
            JSONObject inputPort = new JSONObject();
            inputPort.put("name", field.getName());
            inputPort.put("optional", false); // input port that is not annotated is default to be non-optional
            inputPorts.put(inputPort);
            continue;
          }
          OutputPortFieldAnnotation outputAnnotation = field.getAnnotation(OutputPortFieldAnnotation.class);
          if (outputAnnotation != null) {
            JSONObject outputPort = new JSONObject();
            outputPort.put("name", outputAnnotation.name());
            outputPort.put("optional", outputAnnotation.optional());
            outputPorts.put(outputPort);
            //continue;
          }
          else if (OutputPort.class.isAssignableFrom(field.getType())) {
            JSONObject outputPort = new JSONObject();
            outputPort.put("name", field.getName());
            outputPort.put("optional", true); // output port that is not annotated is default to be optional
            outputPorts.put(outputPort);
            //continue;
          }
        }

        response.put("name", clazz.getCanonicalName());
        response.put("properties", properties);
        response.put("inputPorts", inputPorts);
        response.put("outputPorts", outputPorts);
      }
      catch (JSONException ex) {
        throw new RuntimeException(ex);
      }
      return response;
    }
    else {
      throw new UnsupportedOperationException();
    }
  }

  public static JSONArray getClassProperties(Class<?> clazz) throws IntrospectionException
  {
    return getClassProperties(clazz, 0);
  }

  private static JSONArray getClassProperties(Class<?> clazz, int level) throws IntrospectionException
  {
    JSONArray arr = new JSONArray();
    try {
      for (PropertyDescriptor pd : Introspector.getBeanInfo(clazz).getPropertyDescriptors()) {
        if (!pd.getName().equals("class") && (!(pd.getName().equals("up") && pd.getPropertyType().equals(com.datatorrent.api.Context.class)))) {
          Class<?> propertyType = pd.getPropertyType();
          if (propertyType != null) {
            JSONObject propertyObj = new JSONObject();
            propertyObj.put("name", pd.getName());
            propertyObj.put("canGet", pd.getReadMethod() != null);
            propertyObj.put("canSet", pd.getWriteMethod() != null);
            propertyObj.put("description", pd.getShortDescription());
            propertyObj.put("type", propertyType.getCanonicalName());
            if (!propertyType.isPrimitive() && !propertyType.isEnum() && !propertyType.isArray() && !propertyType.getName().startsWith("java.lang") && level < MAX_PROPERTY_LEVELS) {
              propertyObj.put("properties", getClassProperties(propertyType, level + 1));
            }
            arr.put(propertyObj);
          }
        }
      }
    }
    catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
    return arr;
  }

}
