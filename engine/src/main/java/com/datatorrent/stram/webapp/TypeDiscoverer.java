/*
 *  Copyright (c) 2012-2014 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.webapp;

import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Operator;
import com.google.common.collect.Maps;

/**
 * Introspect operator for generic type parameters and arguments to determine port types.
 */
public class TypeDiscoverer
{
  private static final Logger LOG = LoggerFactory.getLogger(TypeDiscoverer.class);
  // map of generic type name to actual type
  public final Map<String, Type> typeArguments = Maps.newHashMap();

  public static Type getParameterizedTypeArgument(Type type, Class<?> rawType) {
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
          throw new AssertionError("type paramters don't match for " + type);
        }
        for (int i=0; i<typeParameters.length; i++) {
          LOG.debug("{} tv {} bounds {} type arg {}", rawTypeClass.getSimpleName(), typeParameters[i].getName(), typeParameters[i].getBounds(), actualTypeArguments[i]);
          this.typeArguments.put(typeParameters[i].getName(), actualTypeArguments[i]);
        }
      }
    }
  }

  /**
   * Recursively resolve any type parameters against the type arguments in this context.
   * @param type
   * @param typeMeta
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
      meta.put("type", ptype.getRawType().toString());
    } else if (type instanceof WildcardType) {
      meta.put("type", type);
      WildcardType wtype = (WildcardType)type;
      JSONObject wtMeta = new JSONObject();
      wtMeta.put("upper", getTypes(wtype.getUpperBounds()));
      wtMeta.put("lower", getTypes(wtype.getLowerBounds()));
      meta.put("typeBounds", wtMeta);
    } else {
      Type ta = this.typeArguments.get(type.toString());
      meta.put("type", ta != null ? ta.toString() : type.toString());
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

  /**
   * Find the type argument for a given class and parameterized interface
   * the is implemented directly or in a super class or super interface.
   * @param c
   * @param genericInterfaceClass
   * @return
   */
  private static Type findTypeArgument(Class<?> c, Class<?> genericInterfaceClass) {
    while (c != null) {
      // extends generic class?
      Type t = getParameterizedTypeArgument(c.getGenericSuperclass(), genericInterfaceClass);
      if (t != null) {
        return t;
      }
      // implemented interfaces
      Type[] types = c.getGenericInterfaces();
      for (Type interfaceType : types) {
        if ((t = getParameterizedTypeArgument(interfaceType, genericInterfaceClass)) != null) {
          return t;
        }
      }
      // interface that extends parameterized interface?
      for (Class<?> ifClass : c.getInterfaces()) {
        types = ifClass.getGenericInterfaces();
        for (Type interfaceType : types) {
          if ((t = getParameterizedTypeArgument(interfaceType, genericInterfaceClass)) != null) {
            return t;
          }
        }
      }
      c = c.getSuperclass();
    }
    return null;
  }

  private static Type getPortType(Field f)
  {
    if (f.getGenericType() instanceof ParameterizedType) {
      ParameterizedType t = (ParameterizedType)f.getGenericType();
      //LOG.debug("Field type is parameterized: " + Arrays.asList(t.getActualTypeArguments()));
      //LOG.debug("rawType: " + t.getRawType()); // the port class
      Type typeArgument = t.getActualTypeArguments()[0];
      if (typeArgument instanceof Class) {
         return typeArgument;
      } else if (typeArgument instanceof TypeVariable) {
        TypeVariable<?> tv = (TypeVariable<?>)typeArgument;
        LOG.debug("bounds: " + Arrays.asList(tv.getBounds()));
        // variable may contain other variables, java.util.Map<java.lang.String, ? extends T2>
        return tv.getBounds()[0];
      } else if (typeArgument instanceof GenericArrayType) {
        LOG.debug("type {} is of GenericArrayType", typeArgument);
        return typeArgument;
      } else if (typeArgument instanceof WildcardType) {
        LOG.debug("type {} is of WildcardType", typeArgument);
        return typeArgument;
      } else if (typeArgument instanceof ParameterizedType) {
        return typeArgument;
      } else {
        // ports are always parameterized
        throw new IllegalArgumentException("No type variable: " + typeArgument + ", typeParameters: " + Arrays.asList(f.getClass().getTypeParameters()));
      }
    } else {
      LOG.debug("Field is not parameterized: " + f.getGenericType());
      if (Operator.Port.class.isAssignableFrom(f.getType())) {
        Type t = findTypeArgument(f.getType(), Operator.Port.class);
        LOG.debug("Field is of type {}", t);
        return t;
      }
      throw new IllegalArgumentException("Cannot determine type argument for field " + f);
    }
  }

  public JSONArray getPortTypes(Class<?> operatorClass)
  {
    Class<?> c = operatorClass;
    JSONArray ports = new JSONArray();
    while (c != null) {
      Type[] types = c.getGenericInterfaces();
      for (Type type : types) {
        getParameterizedTypeArguments(type);
      }

      Type superClassType = c.getGenericSuperclass();
      if (superClassType != null) {
        getParameterizedTypeArguments(superClassType);
      }

      //TypeVariable<?>[] typeParameters = c.getTypeParameters();
      //for (TypeVariable<?> tv : typeParameters) {
      //  LOG.debug("{} tv {} bounds {}", c.getSimpleName(), tv, tv.getBounds());
      // }
      for (Field f : c.getDeclaredFields()) {
        if (Operator.InputPort.class.isAssignableFrom(f.getType()) || Operator.OutputPort.class.isAssignableFrom(f.getType())) {
          Type t = getPortType(f);
          LOG.debug("field {} class {} type {}", f.getName(), f.getType(), t);
          JSONObject meta = new JSONObject();
          try {
            meta.put("name", f.getName());
            resolveTypeParameters(t, meta);
          } catch (JSONException e) {
            throw new RuntimeException(e);
          }
          ports.put(meta);
        }
      }
      c = c.getSuperclass();
    }
    return ports;
  }
}
