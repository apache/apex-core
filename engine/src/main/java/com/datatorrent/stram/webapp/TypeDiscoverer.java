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
package com.datatorrent.stram.webapp;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;

import com.datatorrent.stram.api.BaseContext;
import com.datatorrent.stram.engine.PortContext;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 * Introspect operator for generic type parameters and arguments to determine port types.
 *
 * @since 2.0.0
 */
public class TypeDiscoverer
{

  enum UI_TYPE
  {

    LIST(Collection.class, "List"),

    ENUM(Enum.class, "Enum"),

    MAP(Map.class, "Map");

    private final Class<?> assignableTo;
    private final String name;

    private UI_TYPE(Class<?> assignableTo, String name)
    {
      this.assignableTo = assignableTo;
      this.name = name;
    }

    public static UI_TYPE getEnumFor(Class<?> clazz)
    {
      if (clazz.isEnum()) {
        return ENUM;
      }
      if (clazz.isArray()) {
        return LIST;
      }
      for (UI_TYPE ui_type : UI_TYPE.values()) {
        if (ui_type.assignableTo.isAssignableFrom(clazz)) {
          return ui_type;
        }
      }
      return null;
    }

    public String getName()
    {
      return name;
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(TypeDiscoverer.class);

  // a predicate that filters out fields that are not attributes
  private static Predicate<Field> attrPredicate = new Predicate<Field>()
  {
    @Override
    public boolean apply(Field input)
    {
      return input.getType().equals(Attribute.class);
    }
  };

  // map of generic type name to actual type
  public final Map<String, Type> typeArguments = Maps.newHashMap();

  public static Type getParameterizedTypeArgument(Type type, Class<?> rawType)
  {
    if (type instanceof ParameterizedType) {
      ParameterizedType ptype = (ParameterizedType)type;
      if (rawType == null || rawType.isAssignableFrom((Class<?>)ptype.getRawType())) {
        return ptype.getActualTypeArguments()[0];
      }
    }
    return null;
  }

  private void getParameterizedTypeArguments(Type type)
  {
    if (type instanceof ParameterizedType) {
      ParameterizedType ptype = (ParameterizedType)type;
      LOG.debug("generic interface or class {} with type parameters {}", type, ptype.getActualTypeArguments());
      Type[] actualTypeArguments = ptype.getActualTypeArguments();
      Type rawType = ptype.getRawType();
      // should always be class
      if (rawType instanceof Class) {
        Class<?> rawTypeClass = (Class<?>)rawType;
        TypeVariable<?>[] typeParameters = rawTypeClass.getTypeParameters();
        if (actualTypeArguments.length != typeParameters.length) {
          throw new AssertionError("type parameters don't match for " + type);
        }
        for (int i = 0; i < typeParameters.length; i++) {
          LOG.debug("{} tv {} bounds {} type arg {}", rawTypeClass.getSimpleName(), typeParameters[i].getName(), typeParameters[i].getBounds(), actualTypeArguments[i]);
          if (!typeArguments.containsKey(typeParameters[i].getName())) {
            this.typeArguments.put(typeParameters[i].getName(), actualTypeArguments[i]);
          }
        }
      }
    }
  }

  /**
   * Recursively resolve any type parameters against the type arguments in this context.
   * @param type
   * @param meta
   */
  private void resolveTypeParameters(Type type, JSONObject meta) throws JSONException
  {
    if (type instanceof ParameterizedType) {
      ParameterizedType ptype = (ParameterizedType)type;
      JSONArray typeArgs = new JSONArray();
      for (Type argType : ptype.getActualTypeArguments()) {
        JSONObject argMeta = new JSONObject();
        resolveTypeParameters(argType, argMeta);
        typeArgs.put(argMeta);
      }
      meta.put("typeArgs", typeArgs);
      meta.put("type", ((Class<?>)ptype.getRawType()).getName());
      UI_TYPE uiType = UI_TYPE.getEnumFor((Class<?>)ptype.getRawType());
      if (uiType != null) {
        meta.put("uiType", uiType.getName());
      }
    } else if (type instanceof GenericArrayType) {
      GenericArrayType gat = (GenericArrayType)type;
      JSONArray typeArgs = new JSONArray();
      JSONObject argMeta = new JSONObject();
      Type componentType = gat.getGenericComponentType();
      componentType = this.typeArguments.get(componentType.toString());
      if (componentType == null) {
        componentType = gat.getGenericComponentType();
      }
      resolveTypeParameters(componentType, argMeta);
      typeArgs.put(argMeta);
      meta.put("typeArgs", typeArgs);
      if (componentType instanceof Class) {
        meta.put("type", Array.newInstance((Class<?>)componentType, 0).getClass().getName());
      } else {
        meta.put("type", Object[].class.getName());
      }
    } else if (type instanceof WildcardType) {
      meta.put("type", type);
      WildcardType wtype = (WildcardType)type;
      JSONObject wtMeta = new JSONObject();
      wtMeta.put("upper", getTypes(wtype.getUpperBounds()));
      wtMeta.put("lower", getTypes(wtype.getLowerBounds()));
      meta.put("typeBounds", wtMeta);
    } else {
      Type ta = this.typeArguments.get(type.toString());
      if (ta == null) {
        ta = type;
      }
      if (ta instanceof Class) {
        meta.put("type", ((Class<?>)ta).getName());
        UI_TYPE uiType = UI_TYPE.getEnumFor(((Class<?>)ta));
        if (uiType != null) {
          meta.put("uiType", uiType.getName());
        }
      } else if (ta instanceof ParameterizedType) {
        resolveTypeParameters(ta, meta);
      } else {
        meta.put("type", ta.toString());
      }

    }
  }

  private JSONArray getTypes(Type[] types)
  {
    JSONArray jsontypes = new JSONArray();
    for (Type upperBound : types) {
      String s = upperBound.toString();
      Type type = this.typeArguments.get(s);
      if (type == null) {
        type = upperBound;
      }
      jsontypes.put(type.toString());
    }
    return jsontypes;
  }

  public void setTypeArguments(Class<?> clazz, Type type, JSONObject meta)
  {
    // TODO: traverse hierarchy and resolve all type parameters
    Type superClassType = clazz.getGenericSuperclass();
    if (superClassType != null) {
      getParameterizedTypeArguments(superClassType);
    }
    getParameterizedTypeArguments(type);
    try {
      resolveTypeParameters(type, meta);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Fetches application attributes.
   *
   * @return all application attributes exposed to user.
   * @throws JSONException
   * @throws IllegalAccessException
   */
  public static JSONObject getAppAttributes() throws JSONException, IllegalAccessException
  {
    Field[] fields = Context.DAGContext.class.getFields();
    Collection<Field> attributes = Collections2.filter(Arrays.asList(fields), attrPredicate);
    return getAttrDescription(new LogicalPlan(), attributes);
  }

  /**
   * Fetches operator attributes.
   *
   * @return all operator attributes exposed to user.
   * @throws JSONException
   * @throws IllegalAccessException
   */
  public static JSONObject getOperatorAttributes() throws JSONException, IllegalAccessException
  {
    Field[] fields = Context.OperatorContext.class.getFields();
    Collection<Field> attributes = Collections2.filter(Arrays.asList(fields), attrPredicate);
    return getAttrDescription(new BaseContext(null, null), attributes);
  }

  /**
   * Fetches port attributes.
   *
   * @return all port attributes exposed to user.
   * @throws JSONException
   * @throws IllegalAccessException
   */
  public static JSONObject getPortAttributes() throws JSONException, IllegalAccessException
  {
    Field[] fields = Context.PortContext.class.getFields();
    Collection<Field> attributes = Collections2.filter(Arrays.asList(fields), attrPredicate);
    return getAttrDescription(new PortContext(null, null), attributes);
  }

  private static JSONObject getAttrDescription(Context context, Collection<Field> attributes) throws JSONException,
      IllegalAccessException
  {
    JSONObject response = new JSONObject();
    JSONArray attrArray = new JSONArray();
    response.put("attributes", attrArray);
    for (Field attrField : attributes) {
      JSONObject attrJson = new JSONObject();
      attrJson.put("name", attrField.getName());
      ParameterizedType attrType = (ParameterizedType)attrField.getGenericType();

      Attribute<?> attr = (Attribute<?>)attrField.get(context);
      Type pType = attrType.getActualTypeArguments()[0];

      TypeDiscoverer discoverer = new TypeDiscoverer();
      discoverer.resolveTypeParameters(pType, attrJson);

      if (attr.defaultValue != null) {
        attrJson.put("default", attr.defaultValue);
      }
      attrArray.put(attrJson);
    }
    return response;
  }

}
