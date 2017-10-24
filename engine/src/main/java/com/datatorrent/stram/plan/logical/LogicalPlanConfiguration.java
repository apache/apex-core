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
package com.datatorrent.stram.plan.logical;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import javax.validation.ValidationException;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.beanutils.BeanMap;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Attribute.AttributeMap.AttributeInitializer;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.GenericOperator;
import com.datatorrent.api.Module;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.StringCodec;
import com.datatorrent.api.StringCodec.JsonStringCodec;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.stram.StramUtils;
import com.datatorrent.stram.client.StramClientUtils;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.ModuleMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OutputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.datatorrent.stram.util.ObjectMapperFactory;

import static org.apache.apex.api.plugin.DAGSetupEvent.Type.POST_CONFIGURE_DAG;
import static org.apache.apex.api.plugin.DAGSetupEvent.Type.POST_POPULATE_DAG;
import static org.apache.apex.api.plugin.DAGSetupEvent.Type.POST_VALIDATE_DAG;
import static org.apache.apex.api.plugin.DAGSetupEvent.Type.PRE_CONFIGURE_DAG;
import static org.apache.apex.api.plugin.DAGSetupEvent.Type.PRE_POPULATE_DAG;
import static org.apache.apex.api.plugin.DAGSetupEvent.Type.PRE_VALIDATE_DAG;

/**
 *
 * Builder for the DAG logical representation of operators and streams from properties.<p>
 * <br>
 * Supports reading as name-value pairs from Hadoop {@link Configuration} or opProps file.
 * <br>
 *
 * @since 0.3.2
 */
public class LogicalPlanConfiguration
{

  private static final Logger LOG = LoggerFactory.getLogger(LogicalPlanConfiguration.class);

  public static final String GATEWAY_PREFIX = StreamingApplication.DT_PREFIX + "gateway.";
  public static final String GATEWAY_LISTEN_ADDRESS = GATEWAY_PREFIX + "listenAddress";

  public static final String STREAM_PREFIX = StreamingApplication.APEX_PREFIX + "stream.";
  public static final String STREAM_SOURCE = "source";
  public static final String STREAM_SINKS = "sinks";
  public static final String STREAM_TEMPLATE = "template";
  public static final String STREAM_LOCALITY = "locality";
  public static final String STREAM_SCHEMA = "schema";

  public static final String OPERATOR_PREFIX = StreamingApplication.APEX_PREFIX + "operator.";
  public static final String OPERATOR_CLASSNAME = "classname";
  public static final String OPERATOR_TEMPLATE = "template";

  public static final String TEMPLATE_idRegExp = "matchIdRegExp";
  public static final String TEMPLATE_appNameRegExp = "matchAppNameRegExp";
  public static final String TEMPLATE_classNameRegExp = "matchClassNameRegExp";

  public static final String CLASS = "class";
  public static final String KEY_SEPARATOR = ".";
  public static final String KEY_SEPARATOR_SPLIT_REGEX = "\\.";

  private static final String CLASS_SUFFIX = "." + CLASS;

  private static final String WILDCARD = "*";
  private static final String WILDCARD_PATTERN = ".*";

  /**
   * This is done to initialize the serial id of these interfaces.
   */
  static {
    Object[] serial = new Object[]{Context.DAGContext.serialVersionUID, OperatorContext.serialVersionUID, PortContext.serialVersionUID};
    LOG.debug("Initialized attributes {}", serial);
  }

  public static final String KEY_APPLICATION_NAME = keyAndDeprecation(Context.DAGContext.APPLICATION_NAME);
  public static final String KEY_GATEWAY_CONNECT_ADDRESS = keyAndDeprecation(Context.DAGContext.GATEWAY_CONNECT_ADDRESS);
  public static final String KEY_GATEWAY_USE_SSL = keyAndDeprecation(Context.DAGContext.GATEWAY_USE_SSL);
  public static final String KEY_GATEWAY_USER_NAME = keyAndDeprecation(Context.DAGContext.GATEWAY_USER_NAME);
  public static final String KEY_GATEWAY_PASSWORD = keyAndDeprecation(Context.DAGContext.GATEWAY_PASSWORD);

  private static String keyAndDeprecation(Attribute<?> attr)
  {
    String key = StreamingApplication.APEX_PREFIX + attr.getName();
    Configuration.addDeprecation(StreamingApplication.DT_PREFIX + attr.getName(), key);
    return key;
  }

  private final DAGSetupPluginManager pluginManager;

  /**
   * This represents an element that can be referenced in a DT property.
   */
  protected enum StramElement
  {
    APPLICATION("application"), GATEWAY("gateway"), TEMPLATE("template"), OPERATOR("operator"), STREAM("stream"),
    PORT("port"), INPUT_PORT("inputport"), OUTPUT_PORT("outputport"),
    ATTR("attr"), PROP("prop"), CLASS("class"), PATH("path"), UNIFIER("unifier");
    private final String value;

    /**
     * Creates a {@link StramElement} with the corresponding name.
     *
     * @param value The name of the {@link StramElement}.
     */
    StramElement(String value)
    {
      this.value = value;
    }

    /**
     * Gets the name of the {@link StramElement}.
     *
     * @return The name of the {@link StramElement}.
     */
    public String getValue()
    {
      return value;
    }

    /**
     * Gets the {@link StramElement} corresponding to the given name.
     *
     * @param value The name for which a {@link StramElement} is desired.
     * @return The {@link StramElement} corresponding to the given name.
     */
    public static StramElement fromValue(String value)
    {
      StramElement velement = null;
      for (StramElement element : StramElement.values()) {
        if (element.getValue().equals(value)) {
          velement = element;
          break;
        }
      }
      return velement;
    }

  }

  /**
   * This is an enum which represents a type of configuration.
   */
  protected enum ConfElement
  {
    STRAM(null, null, null, null),
    APPLICATION(StramElement.APPLICATION, STRAM, null, DAGContext.class),
    TEMPLATE(StramElement.TEMPLATE, STRAM, null, null),
    GATEWAY(StramElement.GATEWAY, ConfElement.APPLICATION, null, null),
    OPERATOR(StramElement.OPERATOR, ConfElement.APPLICATION, null, OperatorContext.class),
    STREAM(StramElement.STREAM, ConfElement.APPLICATION, null, null),
    PORT(StramElement.PORT, ConfElement.OPERATOR, EnumSet.of(StramElement.INPUT_PORT, StramElement.OUTPUT_PORT), PortContext.class),
    UNIFIER(StramElement.UNIFIER, ConfElement.PORT, null, null);

    protected static final Map<StramElement, ConfElement> STRAM_ELEMENT_TO_CONF_ELEMENT = Maps.newHashMap();
    protected static final Map<Class<? extends Context>, ConfElement> CONTEXT_TO_CONF_ELEMENT = Maps.newHashMap();

    static {
      initialize();
    }

    protected static void initialize()
    {
      STRAM.setChildren(Sets.newHashSet(APPLICATION, TEMPLATE));
      APPLICATION.setChildren(Sets.newHashSet(GATEWAY, OPERATOR, STREAM));
      OPERATOR.setChildren(Sets.newHashSet(PORT));
      PORT.setChildren(Sets.newHashSet(UNIFIER));

      STRAM_ELEMENT_TO_CONF_ELEMENT.clear();

      //Initialize StramElement to ConfElement
      for (ConfElement confElement: ConfElement.values()) {
        STRAM_ELEMENT_TO_CONF_ELEMENT.put(confElement.getStramElement(), confElement);

        for (StramElement sElement: confElement.getAllRelatedElements()) {
          STRAM_ELEMENT_TO_CONF_ELEMENT.put(sElement, confElement);
        }
      }

      //Initialize attributes
      for (ConfElement confElement: ConfElement.values()) {
        if (confElement.getParent() == null) {
          continue;
        }

        setAmbiguousAttributes(confElement);
      }

      // build context to conf element map
      CONTEXT_TO_CONF_ELEMENT.clear();

      for (ConfElement confElement: ConfElement.values()) {
        CONTEXT_TO_CONF_ELEMENT.put(confElement.getContextClass(), confElement);
      }

      //Check if all the context classes are accounted for
      Set<Class<? extends Context>> confElementContextClasses = Sets.newHashSet();

      for (ConfElement confElement: ConfElement.values()) {
        if (confElement.getContextClass() == null) {
          continue;
        }

        confElementContextClasses.add(confElement.getContextClass());
      }

      if (!ContextUtils.CONTEXT_CLASSES.equals(confElementContextClasses)) {
        throw new IllegalStateException("All the context classes " + ContextUtils.CONTEXT_CLASSES + " found in "
                                        + Context.class + " are not used by ConfElements " + confElementContextClasses);
      }
    }

    /**
     * This is a recursive method to initialize the ambiguous elements for each
     * {@link ConfElement}.
     *
     * @param element The current {@link ConfElement} at which to start initializing
     * the ambiguous elements.
     * @return The set of all simple attribute names encountered up to this point.
     */
    public static Set<String> setAmbiguousAttributes(ConfElement element)
    {
      Set<String> ambiguousAttributes = Sets.newHashSet();
      Set<String> allChildAttributes = Sets.newHashSet(element.getContextAttributes());

      for (ConfElement childElement: element.getChildren()) {
        Set<String> allAttributes = setAmbiguousAttributes(childElement);
        ambiguousAttributes.addAll(childElement.getAmbiguousAttributes());

        @SuppressWarnings("unchecked")
        Set<String> intersection = Sets.newHashSet(CollectionUtils.intersection(allChildAttributes, allAttributes));
        ambiguousAttributes.addAll(intersection);
        allChildAttributes.addAll(allAttributes);
      }

      element.setAmbiguousAttributes(ambiguousAttributes);
      element.setAllChildAttributes(allChildAttributes);

      return allChildAttributes;
    }

    private final StramElement element;
    private final ConfElement parent;
    private Set<ConfElement> children = Sets.newHashSet();
    private final Set<StramElement> allRelatedElements = Sets.newHashSet();
    private final Class<? extends Context> contextClass;
    private Set<String> ambiguousAttributes = Sets.newHashSet();
    private Set<String> contextAttributes = Sets.newHashSet();
    private Set<String> allChildAttributes = Sets.newHashSet();

    /**
     * This creates a {@link ConfElement}.
     *
     * @param element The current {@link StramElement} representing a {@link ConfElement}.
     * @param parent The parent {@link ConfElement}.
     * @param additionalRelatedElements Any additional {@link StramElement} that could be
     * related to this {@link ConfElement}.
     * @param contextClass The {@link Context} class that contains all the attributes to
     * be used by this {@link ConfElement}.
     */
    ConfElement(StramElement element,
        ConfElement parent,
        Set<StramElement> additionalRelatedElements,
        Class<? extends Context> contextClass)
    {
      this.element = element;
      this.parent = parent;

      if (additionalRelatedElements != null) {
        this.allRelatedElements.addAll(additionalRelatedElements);
      }

      this.allRelatedElements.add(element);

      this.contextClass = contextClass;

      this.contextAttributes = contextClass != null ? ContextUtils.CONTEXT_CLASS_TO_ATTRIBUTES.get(contextClass) : new HashSet<String>();
    }

    private void setAllChildAttributes(Set<String> allChildAttributes)
    {
      this.allChildAttributes = Preconditions.checkNotNull(allChildAttributes);
    }

    public Set<String> getAllChildAttributes()
    {
      return allChildAttributes;
    }

    private void setAmbiguousAttributes(Set<String> ambiguousAttributes)
    {
      this.ambiguousAttributes = Preconditions.checkNotNull(ambiguousAttributes);
    }

    /**
     * Gets the simple names of attributes which are specified under multiple configurations which
     * include this configuration or any child configurations.
     *
     * @return The set of ambiguous simple attribute names.
     */
    public Set<String> getAmbiguousAttributes()
    {
      return ambiguousAttributes;
    }

    /**
     * Gets the {@link Context} class that corresponds to this {@link ConfElement}.
     *
     * @return The {@link Context} class that corresponds to this {@link ConfElement}.
     */
    public Class<? extends Context> getContextClass()
    {
      return contextClass;
    }

    /**
     * Gets the {@link StramElement} representing this {@link ConfElement}.
     *
     * @return The {@link StramElement} corresponding to this {@link ConfElement}.
     */
    public StramElement getStramElement()
    {
      return element;
    }

    /**
     * Gets the attributes contained in the {@link Context} associated with this {@link ConfElement}.
     *
     * @return A {@link java.util.Set} containing the simple attribute names of all of the attributes
     * contained in the {@link Context} associated with this {@link ConfElement}.
     */
    public Set<String> getContextAttributes()
    {
      return contextAttributes;
    }

    /**
     * Gets the {@link ConfElement} that is the parent of this {@link ConfElement}.
     *
     * @return The {@link ConfElement} that is the parent of this {@link ConfElement}.
     */
    public ConfElement getParent()
    {
      return parent;
    }

    /**
     * Sets the child {@link ConfElement}s of this {@link ConfElement}.
     *
     * @param children The child {@link ConfElement}s of this {@link ConfElement}.
     */
    private void setChildren(Set<ConfElement> children)
    {
      this.children = Preconditions.checkNotNull(children);
    }

    /**
     * Gets the child {@link ConfElement}s of this {@link ConfElement}.
     *
     * @return The child {@link ConfElement} of this {@link ConfElement}
     */
    public Set<ConfElement> getChildren()
    {
      return children;
    }

    /**
     * Gets all the {@link StramElement}s that are represented by this {@link ConfElement}.
     *
     * @return All the {@link StramElement}s that are represented by this {@link ConfElement}.
     */
    public Set<StramElement> getAllRelatedElements()
    {
      return allRelatedElements;
    }

    /**
     * Gets the {@link StramElement} representing the {@link Conf} type which can be a parent of the {@link Conf} type
     * represented by the given {@link StramElement}.
     *
     * @param conf The {@link StramElement} representing the {@link Conf} type of interest.
     * @return The {@link StramElement} representing the {@link Conf} type which can be a parent of the given {@link Conf} type.
     */
    public static StramElement getAllowedParentConf(StramElement conf)
    {
      ConfElement confElement = STRAM_ELEMENT_TO_CONF_ELEMENT.get(conf);

      if (confElement == null) {
        throw new IllegalArgumentException(conf + " is not a valid conf element.");
      }

      return confElement.getParent().getStramElement();
    }

    /**
     * Creates a list of {@link StramElement}s which represent the path from the current {@link Conf} type to
     * a root {@link Conf} type. This path includes the current {@link Conf} type as well as the root.
     *
     * @param conf The current {@link Conf} type.
     * @return A path from the current {@link Conf} type to a root {@link Conf} type, which includes the current and root
     * {@link Conf} types.
     */
    public static List<StramElement> getPathFromChildToRootInclusive(StramElement conf)
    {
      ConfElement confElement = STRAM_ELEMENT_TO_CONF_ELEMENT.get(conf);

      if (confElement == null) {
        throw new IllegalArgumentException(conf + " does not represent a valid configuration type.");
      }

      List<StramElement> path = Lists.newArrayList();

      for (; confElement != null; confElement = confElement.getParent()) {
        path.add(confElement.getStramElement());
      }

      return path;
    }

    /**
     * Creates a list of {@link StramElement}s which represent the path from the root {@link Conf} type to
     * the current {@link Conf} type. This path includes the root {@link Conf} type as well as the current {@link Conf} type.
     *
     * @param conf The current {@link Conf} type.
     * @return A path from the root {@link Conf} type to the current {@link Conf} type, which includes the current and root
     * {@link Conf} types.
     */
    public static List<StramElement> getPathFromRootToChildInclusive(StramElement conf)
    {
      List<StramElement> path = getPathFromChildToRootInclusive(conf);
      return Lists.reverse(path);
    }

    /**
     * Creates a list of {@link StramElement}s which represent the path from the current {@link Conf} type to
     * a parent {@link Conf} type. This path includes the current {@link Conf} type as well as the parent.
     *
     * @param child The current {@link Conf} type.
     * @param parent The parent {@link Conf} type.
     * @return A path from the current {@link Conf} type to a parent {@link Conf} type, which includes the current and parent
     * {@link Conf} types.
     */
    public static List<StramElement> getPathFromChildToParentInclusive(StramElement child, StramElement parent)
    {
      ConfElement confElement = STRAM_ELEMENT_TO_CONF_ELEMENT.get(child);

      if (confElement == null) {
        throw new IllegalArgumentException(child + " does not represent a valid configuration type.");
      }

      List<StramElement> path = Lists.newArrayList();

      if (child == parent) {
        path.add(child);
        return path;
      }

      for (; confElement != null; confElement = confElement.getParent()) {
        path.add(confElement.getStramElement());

        if (confElement.getStramElement() == parent) {
          break;
        }
      }

      if (path.get(path.size() - 1) != parent) {
        throw new IllegalArgumentException(parent + " is not a valid parent of " + child);
      }

      return path;
    }

    /**
     * Creates a list of {@link StramElement}s which represent the path from the parent {@link Conf} type to
     * a child {@link Conf} type. This path includes the parent {@link Conf} type as well as the current {@link Conf} type.
     *
     * @param child The current {@link Conf} type.
     * @param parent The parent {@link Conf} type.
     * @return A path from the parent {@link Conf} type to the current {@link Conf} type, which includes the current and parent
     * {@link Conf} types.
     */
    public static List<StramElement> getPathFromParentToChildInclusive(StramElement child, StramElement parent)
    {
      List<StramElement> path = getPathFromChildToParentInclusive(child, parent);
      return Lists.reverse(path);
    }

    /**
     * This method searches the current {@link ConfElement} and its children to find a {@link ConfElement}
     * that contains the given simple {@link Attribute} name.
     *
     * @param current The current {@link ConfElement}.
     * @param simpleAttributeName The simple {@link Attribute} name to search for.
     * @return The {@link ConfElement} that contains the given attribute, or null if no {@link ConfElement} contains
     * the given attribute.
     */
    public static ConfElement findConfElementWithAttribute(ConfElement current, String simpleAttributeName)
    {
      if (current.getContextAttributes().contains(simpleAttributeName)) {
        return current;
      }

      for (ConfElement childConfElement: current.getChildren()) {
        ConfElement result = findConfElementWithAttribute(childConfElement, simpleAttributeName);

        if (result != null) {
          return result;
        }
      }

      return null;
    }

    protected static Conf addConfs(Conf parentConf, ConfElement childConfElement)
    {
      //Figure out what configurations need to be added to hold this attribute
      List<StramElement> path = ConfElement.getPathFromParentToChildInclusive(childConfElement.getStramElement(), parentConf.getConfElement().getStramElement());

      for (int pathIndex = 1; pathIndex < path.size(); pathIndex++) {
        LOG.debug("Adding conf");
        StramElement pathElement = path.get(pathIndex);
        //Add the configurations we need to hold this attribute
        parentConf = addConf(pathElement, WILDCARD, parentConf);
      }

      return parentConf;
    }

  }

  /**
   * Utility class that holds methods for handling {@link Context} classes.
   */
  @SuppressWarnings("unchecked")
  protected static class ContextUtils
  {
    private static final Map<String, Type> ATTRIBUTES_TO_TYPE = Maps.newHashMap();
    public static final Map<Class<? extends Context>, Set<String>> CONTEXT_CLASS_TO_ATTRIBUTES = Maps.newHashMap();
    public static final Set<Class<? extends Context>> CONTEXT_CLASSES = Sets.newHashSet();
    public static final Map<Class<? extends Context>, Map<String, Attribute<?>>> CONTEXT_TO_ATTRIBUTE_NAME_TO_ATTRIBUTE = Maps.newHashMap();

    static {
      initialize();
    }

    @VisibleForTesting
    protected static void initialize()
    {
      CONTEXT_CLASSES.clear();

      for (Class<?> clazz: Context.class.getDeclaredClasses()) {
        if (!Context.class.isAssignableFrom(clazz)) {
          continue;
        }

        CONTEXT_CLASSES.add((Class<? extends Context>)clazz);
      }

      buildAttributeMaps(CONTEXT_CLASSES);
    }

    @VisibleForTesting
    protected static void buildAttributeMaps(Set<Class<? extends Context>> contextClasses)
    {
      CONTEXT_CLASS_TO_ATTRIBUTES.clear();
      CONTEXT_TO_ATTRIBUTE_NAME_TO_ATTRIBUTE.clear();
      ATTRIBUTES_TO_TYPE.clear();

      for (Class<? extends Context> contextClass: contextClasses) {
        Set<String> contextAttributes = Sets.newHashSet();

        Field[] fields = contextClass.getDeclaredFields();

        for (Field field: fields) {
          if (!Attribute.class.isAssignableFrom(field.getType())) {
            continue;
          }

          Type fieldType = ((ParameterizedType)field.getGenericType()).getActualTypeArguments()[0];
          contextAttributes.add(field.getName());

          Type existingType = ATTRIBUTES_TO_TYPE.get(field.getName());

          if (existingType != null && !existingType.equals(fieldType)) {
            throw new ValidationException("The attribute " + field.getName() +
                " is defined with two different types in two different context classes: " +
                fieldType + " and " + existingType + "\n" +
                "Attributes with the same name are required to have the same type accross all Context classes.");
          }

          ATTRIBUTES_TO_TYPE.put(field.getName(), fieldType);
        }

        CONTEXT_CLASS_TO_ATTRIBUTES.put(contextClass, contextAttributes);
      }

      for (Class<? extends Context> contextClass: contextClasses) {
        Map<String, Attribute<?>> simpleAttributeNameToAttribute = Maps.newHashMap();
        CONTEXT_TO_ATTRIBUTE_NAME_TO_ATTRIBUTE.put(contextClass, simpleAttributeNameToAttribute);

        Set<Attribute<Object>> attributes = AttributeInitializer.getAttributes(contextClass);

        LOG.debug("context class {} and attributes {}", contextClass, attributes);

        for (Attribute<Object> attribute: attributes) {
          simpleAttributeNameToAttribute.put(AttributeParseUtils.getSimpleName(attribute), attribute);
        }
      }
    }

    private ContextUtils()
    {
      //Private construct to prevent instantiation of utility class
    }

    /**
     * This method is only used for testing.
     *
     * @param contextClass
     * @param attribute
     */
    @VisibleForTesting
    protected static void addAttribute(Class<? extends Context> contextClass, Attribute<?> attribute)
    {
      Set<String> attributeNames = CONTEXT_CLASS_TO_ATTRIBUTES.get(contextClass);

      if (attributeNames == null) {
        attributeNames = Sets.newHashSet();
        CONTEXT_CLASS_TO_ATTRIBUTES.put(contextClass, attributeNames);
      }

      attributeNames.add(attribute.getSimpleName());

      CONTEXT_CLASSES.add(contextClass);
      Map<String, Attribute<?>> attributeMap = CONTEXT_TO_ATTRIBUTE_NAME_TO_ATTRIBUTE.get(contextClass);

      if (attributeMap == null) {
        attributeMap = Maps.newHashMap();
        CONTEXT_TO_ATTRIBUTE_NAME_TO_ATTRIBUTE.put(contextClass, attributeMap);
      }

      attributeMap.put(attribute.getSimpleName(), attribute);
    }

    /**
     * This method is only used for testing.
     *
     * @param contextClass
     * @param attribute
     */
    @VisibleForTesting
    protected static void removeAttribute(Class<? extends Context> contextClass, Attribute<?> attribute)
    {
      Set<String> attributeNames = CONTEXT_CLASS_TO_ATTRIBUTES.get(contextClass);

      if (attributeNames != null) {
        attributeNames.remove(attribute.getSimpleName());

        if (attributeNames.isEmpty()) {
          CONTEXT_CLASS_TO_ATTRIBUTES.remove(contextClass);
        }
      }

      if (!CONTEXT_CLASS_TO_ATTRIBUTES.keySet().contains(contextClass)) {
        CONTEXT_CLASSES.remove(contextClass);
      }

      Map<String, Attribute<?>> attributeMap = CONTEXT_TO_ATTRIBUTE_NAME_TO_ATTRIBUTE.get(contextClass);

      if (attributeMap != null) {
        attributeMap.remove(attribute.getSimpleName());

        if (attributeMap.isEmpty()) {
          CONTEXT_TO_ATTRIBUTE_NAME_TO_ATTRIBUTE.remove(contextClass);
        }
      }
    }

  }

  /**
   * Utility class that holds methods for parsing.
   */
  protected static class AttributeParseUtils
  {
    public static final Set<String> ALL_SIMPLE_ATTRIBUTE_NAMES;

    static {
      ALL_SIMPLE_ATTRIBUTE_NAMES = Sets.newHashSet();

      initialize();
    }

    public static void initialize()
    {
      ALL_SIMPLE_ATTRIBUTE_NAMES.clear();

      for (Map.Entry<Class<? extends Context>, Set<String>> entry: ContextUtils.CONTEXT_CLASS_TO_ATTRIBUTES.entrySet()) {
        ALL_SIMPLE_ATTRIBUTE_NAMES.addAll(entry.getValue());
      }
    }

    private AttributeParseUtils()
    {
      //Private construct to prevent instantiation of utility class
    }

    /**
     * This method creates all the appropriate child {@link Conf}s of the given parent {@link Conf} and adds the given
     * attribute to the parent {@link Conf} if appropriate as well as all the child {@link Conf}s of the parent if
     * appropriate.
     *
     * @param conf The parent {@link Conf}.
     * @param attributeName The simple name of the attribute to add.
     * @param attrValue The value of the attribute.
     */
    protected static void processAllConfsForAttribute(Conf conf, String attributeName, String attrValue)
    {
      ConfElement confElement = conf.getConfElement();

      LOG.debug("Current confElement {} and name {}", confElement.getStramElement(), conf.getId());

      if (confElement.getContextAttributes().contains(attributeName)) {
        LOG.debug("Adding attribute");
        @SuppressWarnings("unchecked")
        Attribute<Object> attr = (Attribute<Object>)ContextUtils.CONTEXT_TO_ATTRIBUTE_NAME_TO_ATTRIBUTE.get(confElement.getContextClass()).get(attributeName);
        conf.setAttribute(attr, attrValue);
      }

      for (ConfElement childConfElement: confElement.getChildren()) {

        if (!childConfElement.getAllChildAttributes().contains(attributeName)) {
          continue;
        }

        Conf childConf = addConf(childConfElement.getStramElement(), WILDCARD, conf);
        processAllConfsForAttribute(childConf, attributeName, attrValue);
      }
    }

    /**
     * This extracts the name of an attribute from the given set of keys.
     *
     * @param element The {@link StramElement} corresponding to the current element being parsed.
     * @param keys The split keys that are being parse.
     * @param index The current key that the parser is on.
     * @return The FQN name of an attribute or just the name of an Attribute.
     */
    public static String getAttributeName(StramElement element, String[] keys, int index)
    {

      if (element != null && element != StramElement.ATTR) {
        throw new IllegalArgumentException("The given " + StramElement.class + " must either have a value of null or " + StramElement.ATTR + " but it had a value of " + element);
      }

      String attributeName;

      if (element == StramElement.ATTR) {
        attributeName = getCompleteKey(keys, index + 1);
      } else {
        attributeName = getCompleteKey(keys, index);
      }

      return attributeName;
    }

    /**
     * This method checks to see if the attribute name is simple or is prefixed with the FQCN of the {@link Context}
     * class which contains it.
     *
     * @param attributeName The attribute name to check.
     * @return True if the attribute name is simple. False otherwise.
     */
    public static boolean isSimpleAttributeName(String attributeName)
    {
      return !attributeName.contains(KEY_SEPARATOR);
    }

    /**
     * Gets the {@link Context} class that the given attributeName belongs to.
     *
     * @param attributeName The {@link Attribute} name whose {@link Context} class needs to be
     * discovered.
     * @return The {@link Context} class that the given {@link Attribute} name belongs to.
     */
    @SuppressWarnings("unchecked")
    public static Class<? extends Context> getContainingContextClass(String attributeName)
    {
      if (isSimpleAttributeName(attributeName)) {
        throw new IllegalArgumentException("The given attribute name " + attributeName + " is simple.");
      }

      LOG.debug("Attribute Name {}", attributeName);

      int lastSeparator = attributeName.lastIndexOf(KEY_SEPARATOR);
      String contextClassName = attributeName.substring(0, lastSeparator);

      int lastPeriod = contextClassName.lastIndexOf(KEY_SEPARATOR);

      StringBuilder sb = new StringBuilder(contextClassName);
      sb.setCharAt(lastPeriod, '$');
      contextClassName = sb.toString();

      Class<? extends Context> contextClass;

      try {
        Class<?> clazz = Class.forName(contextClassName);

        if (Context.class.isAssignableFrom(clazz)) {
          contextClass = (Class<? extends Context>)clazz;
        } else {
          throw new IllegalArgumentException("The provided context class name " + contextClassName + " is not valid.");
        }
      } catch (ClassNotFoundException ex) {
        throw new IllegalArgumentException(ex);
      }

      String simpleAttributeName = getSimpleAttributeName(attributeName);

      if (!ContextUtils.CONTEXT_CLASS_TO_ATTRIBUTES.get(contextClass).contains(simpleAttributeName)) {
        throw new ValidationException(simpleAttributeName + " is not a valid attribute of " + contextClass);
      }

      return contextClass;
    }

    /**
     * This extract this simple {@link Attribute} name from the given {@link Attribute} name.
     *
     * @param attributeName The attribute name to extract a simple attribute name from.
     * @return The simple attribute name.
     */
    public static String getSimpleAttributeName(String attributeName)
    {
      if (isSimpleAttributeName(attributeName)) {
        return attributeName;
      }

      if (attributeName.endsWith(KEY_SEPARATOR)) {
        throw new IllegalArgumentException("The given attribute name ends with \"" + KEY_SEPARATOR + "\" so a simple name cannot be extracted.");
      }

      return attributeName.substring(attributeName.lastIndexOf(KEY_SEPARATOR) + 1, attributeName.length());
    }

    /**
     * Gets the simple name of an {@link Attribute}, which does not include the FQCN of the {@link Context} class
     * which contains it.
     *
     * @param attribute The {@link Attribute} of interest.
     * @return The name of an {@link Attribute}.
     */
    public static String getSimpleName(Attribute<?> attribute)
    {
      return getSimpleAttributeName(attribute.name);
    }

  }

  public class JSONObject2String implements StringCodec<Object>, Serializable
  {
    private static final long serialVersionUID = -664977453308585878L;

    @Override
    public Object fromString(String jsonObj)
    {
      LOG.debug("JONString {}", jsonObj);
      ObjectMapper mapper = ObjectMapperFactory.getOperatorValueDeserializer();
      try {
        return mapper.readValue(jsonObj, Object.class);
      } catch (IOException e) {
        throw new RuntimeException("Error parsing json content", e);
      }
    }

    @Override
    public String toString(Object pojo)
    {
      ObjectMapper mapper = ObjectMapperFactory.getOperatorValueDeserializer();
      try {
        return mapper.writeValueAsString(pojo);
      } catch (IOException e) {
        throw new RuntimeException("Error writing object as json", e);
      }
    }

  }

  /**
   * This is an abstract class representing the configuration applied to an element in the DAG.
   */
  private abstract static class Conf
  {
    protected Conf parentConf = null;

    protected final Map<Attribute<Object>, String> attributes = Maps.newHashMap();
    protected final PropertiesWithModifiableDefaults properties = new PropertiesWithModifiableDefaults();

    protected Map<StramElement, Map<String, ? extends Conf>> children = Maps.newHashMap();

    protected String id;

    public void setId(String id)
    {
      this.id = id;
    }

    public String getId()
    {
      return id;
    }

    public void setParentConf(Conf parentConf)
    {
      this.parentConf = parentConf;
    }

    @SuppressWarnings("unchecked")
    public <T extends Conf> T getParentConf()
    {
      return (T)parentConf;
    }

    /**
     * Gets an ancestor {@link Conf} of this {@link Conf} of the given {@link StramElement} type.
     *
     * @param <T>             The {@link Conf} Class of the ancestor conf
     * @param ancestorElement The {@link StramElement} representing the type of the ancestor {@link Conf}.
     * @return The ancestor {@link Conf} of the corresponding {@link StramElement} type, or null if no ancestor
     * {@link Conf} with
     * the given {@link StramElement} type exists.
     */
    @SuppressWarnings("unchecked")
    public <T extends Conf> T getAncestorConf(StramElement ancestorElement)
    {
      if (getConfElement().getStramElement() == ancestorElement) {
        return (T)this;
      }
      if (parentConf == null) {
        return null;
      } else {
        return parentConf.getAncestorConf(ancestorElement);
      }
    }

    /**
     * This method retrieves a child {@link Conf} of the given {@link StramElement} type with the given name. If
     * a child {@link Conf} with the given name and {@link StramElement} type doesn't exist, then it is added.
     * @param <T> The type of the child {@link Conf}.
     * @param id The name of the child {@link Conf}.
     * @param childType The {@link StramElement} representing the type of the child {@link Conf}.
     * @param clazz The {@link java.lang.Class} of the child {@link Conf} to add if a {@link Conf} of the given id
     * and {@link StramElement} type is not present.
     * @return A child {@link Conf} of this {@link Conf} with the given id and {@link StramElement} type.
     */
    public <T extends Conf> T getOrAddChild(String id, StramElement childType, Class<T> clazz)
    {
      @SuppressWarnings("unchecked")
      Map<String, T> elChildren = (Map<String, T>)children.get(childType);
      if (elChildren == null) {
        elChildren = Maps.newHashMap();
        children.put(childType, elChildren);
      }
      T conf = getOrAddConf(elChildren, id, clazz);
      if (conf != null) {
        conf.setParentConf(this);
      }
      return conf;
    }

    public void setAttribute(Attribute<Object> attr, String value)
    {
      attributes.put(attr, value);
    }

    public void setProperty(String name, String value)
    {
      properties.setProperty(name, value);
    }

    public void setDefaultProperties(Properties defaults)
    {
      properties.setDefaultProperties(defaults);
    }

    /**
     * This method returns a list of all the child {@link Conf}s of this {@link Conf} with the matching name
     * and {@link StramElement} type.
     * @param <T> The types of the child {@link Conf}s.
     * @param name The name of the child {@link Conf}s to return. If the name of the specified child {@link Conf}
     * is null then configurations with the name specified as a {@link LogicalPlanConfiguration#WILDCARD} are matched.
     * @param childType The {@link StramElement} corresponding to the type of a child {@link Conf}.
     * @return The list of child {@link Conf}s with a matching name and {@link StramElement} type.
     */
    public <T extends Conf> List<T> getMatchingChildConf(String name, StramElement childType)
    {
      List<T> childConfs = new ArrayList<>();
      Map<String, T> elChildren = getChildren(childType);
      for (Map.Entry<String, T> entry : elChildren.entrySet()) {
        String key = entry.getKey();
        boolean match = false;
        boolean exact = false;
        // Match WILDCARD to null
        if (name == null) {
          if (key == null) {
            match = true;
            exact = true;
          } else if (key.equals(WILDCARD)) {
            match = true;
          }
        } else {
          // Also treat WILDCARD as match any character string when running regular express match
          if (key.equals(WILDCARD)) {
            key = WILDCARD_PATTERN;
          }
          if (name.matches(key)) {
            match = true;
          }
          if (name.equals(key)) {
            exact = true;
          }
        }
        // There will be a better match preference order
        if (match) {
          if (!exact) {
            childConfs.add(entry.getValue());
          } else {
            childConfs.add(0, entry.getValue());
          }
        }
      }
      return childConfs;
    }

    /**
     * Returns the {@link Conf} corresponding to the given id from the given map. If a {@link Conf} with the
     * given id is not present in the given map, then a new {@link Conf} of the given class is created and added
     * to the map.
     * @param <T> The type of the {@link Conf}s contained in the map.
     * @param map The map to retrieve a {@link Conf} from or add a {@link Conf} to.
     * @param id The name of the {@link Conf} to retrieve from or add to the given map.
     * @param clazz The {@link java.lang.Class} of the {@link Conf} to add to the given map, if a {@link Conf} with
     * the given name is not present in the given map.
     * @return A {@link Conf} with the given name, contained in the given map.
     */
    protected <T extends Conf> T getOrAddConf(Map<String, T> map, String id, Class<T> clazz)
    {
      T conf = map.get(id);
      if (conf == null) {
        try {
          Constructor<T> declaredConstructor = clazz.getDeclaredConstructor(new Class<?>[]{});
          conf = declaredConstructor.newInstance(new Object[]{});
          conf.setId(id);
          map.put(id, conf);
        } catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException |
            SecurityException | InvocationTargetException e) {
          LOG.error("Error instantiating configuration", e);
        }
      }
      return conf;
    }

    public <T extends Conf> T getChild(String id, StramElement childType)
    {
      T conf = null;
      @SuppressWarnings("unchecked")
      Map<String, T> elChildren = (Map<String, T>)children.get(childType);
      if (elChildren != null) {
        conf = elChildren.get(id);
      }
      return conf;
    }

    @SuppressWarnings("unchecked")
    public <T extends Conf> Map<String, T> getChildren(StramElement childType)
    {
      // Always return non null so caller will not have to do extra check as expected
      Map<String, T> elChildren = (Map<String, T>)children.get(childType);
      if (elChildren == null) {
        elChildren = Maps.newHashMap();
        children.put(childType, elChildren);
      }
      return elChildren;
    }

    // Override for parsing of custom elements other than attributes and opProps
    // Make this config parse element as the entry point for parsing in future instead of the generic method in
    // parent class
    public void parseElement(StramElement element, String[] keys, int index, String propertyValue)
    {
    }

    public boolean isAllowedChild(StramElement childType)
    {
      StramElement[] childElements = getChildElements();
      if (childElements != null) {
        for (StramElement childElement : childElements) {
          if (childType == childElement) {
            return true;
          }
        }
      }
      return false;
    }

    public StramElement getDefaultChildElement()
    {
      if ((getConfElement().getContextClass() == null) && isAllowedChild(StramElement.PROP)) {
        return StramElement.PROP;
      }
      return null;
    }

    public boolean ignoreUnknownChildren()
    {
      return getDefaultChildElement() == null;
    }

    public abstract StramElement[] getChildElements();

    public abstract ConfElement getConfElement();
  }

  private static class StramConf extends Conf
  {

    private final Map<String, String> appAliases = Maps.newHashMap();

    private static final StramElement[] CHILD_ELEMENTS = new StramElement[]{StramElement.APPLICATION,
        StramElement.GATEWAY, StramElement.TEMPLATE, StramElement.OPERATOR,
        StramElement.PORT, StramElement.INPUT_PORT, StramElement.OUTPUT_PORT, StramElement.STREAM,
        StramElement.TEMPLATE, StramElement.ATTR, StramElement.UNIFIER};

    StramConf()
    {
    }

    @Override
    public StramElement[] getChildElements()
    {
      return CHILD_ELEMENTS;
    }

    @Override
    public ConfElement getConfElement()
    {
      return ConfElement.STRAM;
    }
  }

  /**
   * This holds the configuration information for an Apex application.
   */
  private static class AppConf extends Conf
  {

    private static final StramElement[] CHILD_ELEMENTS = new StramElement[]{StramElement.GATEWAY, StramElement.OPERATOR,
        StramElement.PORT, StramElement.INPUT_PORT, StramElement.OUTPUT_PORT, StramElement.STREAM, StramElement.ATTR,
        StramElement.CLASS, StramElement.PATH, StramElement.PROP, StramElement.UNIFIER};

    @SuppressWarnings("unused")
    AppConf()
    {
    }

    @Override
    public void parseElement(StramElement element, String[] keys, int index, String propertyValue)
    {
      if ((element == StramElement.CLASS) || (element == StramElement.PATH)) {
        StramConf stramConf = getParentConf();
        stramConf.appAliases.put(propertyValue, getId());
      }
    }

    @Override
    public StramElement[] getChildElements()
    {
      return CHILD_ELEMENTS;
    }

    @Override
    public StramElement getDefaultChildElement()
    {
      return StramElement.PROP;
    }

    @Override
    public ConfElement getConfElement()
    {
      return ConfElement.APPLICATION;
    }
  }

  private static class GatewayConf extends Conf
  {

    private static final StramElement[] CHILD_ELEMENTS = new StramElement[]{StramElement.PROP};

    @SuppressWarnings("unused")
    GatewayConf()
    {
    }

    @Override
    public StramElement[] getChildElements()
    {
      return CHILD_ELEMENTS;
    }

    @Override
    public ConfElement getConfElement()
    {
      return ConfElement.GATEWAY;
    }
  }

  /**
   * Named set of opProps that can be used to instantiate streams or operators
   * with common settings.
   */
  private static class TemplateConf extends Conf
  {

    private static final StramElement[] CHILD_ELEMENTS = new StramElement[]{StramElement.PROP};

    @SuppressWarnings("unused")
    TemplateConf()
    {
    }

    @Override
    public StramElement[] getChildElements()
    {
      return CHILD_ELEMENTS;
    }

    @Override
    public ConfElement getConfElement()
    {
      return ConfElement.TEMPLATE;
    }

    @Override
    public void setProperty(String name, String value)
    {
      if (name.equals(TEMPLATE_appNameRegExp)) {
        appNameRegExp = value;
      } else if (name.equals(TEMPLATE_idRegExp)) {
        idRegExp = value;
      } else if (name.equals(TEMPLATE_classNameRegExp)) {
        classNameRegExp = value;
      } else {
        super.setProperty(name, value);
      }
    }

    private String idRegExp;
    private String appNameRegExp;
    private String classNameRegExp;

  }

  /**
   * This holds the configuration information for a stream that connects two operators in an Apex application.
   */
  private static class StreamConf extends Conf
  {

    private static final StramElement[] CHILD_ELEMENTS = new StramElement[]{StramElement.TEMPLATE, StramElement.PROP};

    private OperatorConf sourceNode;
    private final Set<OperatorConf> targetNodes = new HashSet<>();

    @SuppressWarnings("unused")
    StreamConf()
    {
    }

    @Override
    public ConfElement getConfElement()
    {
      return ConfElement.STREAM;
    }

    /**
     * Locality for adjacent operators.
     * @return boolean
     */
    public DAG.Locality getLocality()
    {
      String v = properties.getProperty(STREAM_LOCALITY, null);
      return (v != null) ? DAG.Locality.valueOf(v) : null;
    }

    /**
     * Set source on stream to the node output port.
     * @param portName
     * @param node
     */
    public StreamConf setSource(String portName, OperatorConf node)
    {
      if (this.sourceNode != null) {
        throw new IllegalArgumentException(String.format("Stream already receives input from %s", sourceNode));
      }
      node.outputs.put(portName, this);
      this.sourceNode = node;
      return this;
    }

    public StreamConf addSink(String portName, OperatorConf targetNode)
    {
      if (targetNode.inputs.containsKey(portName)) {
        throw new IllegalArgumentException(String.format("Port %s already connected to stream %s", portName, targetNode.inputs.get(portName)));
      }
      //LOG.debug("Adding {} to {}", targetNode, this);
      targetNode.inputs.put(portName, this);
      targetNodes.add(targetNode);
      return this;
    }

    @Override
    public void setProperty(String name, String value)
    {
      AppConf appConf = getParentConf();
      if (STREAM_SOURCE.equals(name)) {
        if (sourceNode != null) {
          // multiple sources not allowed
          //throw new IllegalArgumentException("Duplicate " + propertyName);
          throw new IllegalArgumentException("Duplicate " + name);
        }
        String[] parts = getNodeAndPortId(value);
        setSource(parts[1], appConf.getOrAddChild(parts[0], StramElement.OPERATOR, OperatorConf.class));
      } else if (STREAM_SINKS.equals(name)) {
        String[] targetPorts = value.split(",");
        for (String nodeAndPort : targetPorts) {
          String[] parts = getNodeAndPortId(nodeAndPort.trim());
          addSink(parts[1], appConf.getOrAddChild(parts[0], StramElement.OPERATOR, OperatorConf.class));
        }
      } else if (STREAM_TEMPLATE.equals(name)) {
        StramConf stramConf = getAncestorConf(null);
        TemplateConf templateConf = (TemplateConf)stramConf.getOrAddChild(value, StramElement.TEMPLATE, elementMaps.get(StramElement.TEMPLATE));
        setDefaultProperties(templateConf.properties);
      } else {
        super.setProperty(name, value);
      }
    }

    private String[] getNodeAndPortId(String s)
    {
      String[] parts = s.split("\\.");
      if (parts.length != 2) {
        throw new IllegalArgumentException("Invalid node.port reference: " + s);
      }
      return parts;
    }

    @Override
    public StramElement[] getChildElements()
    {
      return CHILD_ELEMENTS;
    }

    @Override
    public String toString()
    {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
          .append("id", this.id)
          .toString();
    }

  }

  /**
   * This is a simple extension of {@link java.util.Properties} which allows you to specify default properties.
   */
  private static class PropertiesWithModifiableDefaults extends Properties
  {
    private static final long serialVersionUID = -4675421720308249982L;

    /**
     * @param defaults
     */
    void setDefaultProperties(Properties defaults)
    {
      super.defaults = defaults;
    }
  }

  /**
   * This holds the configuration information for an operator in an Apex application.
   */
  private static class OperatorConf extends Conf
  {
    private static final StramElement[] CHILD_ELEMENTS = new StramElement[]{StramElement.PORT,
        StramElement.INPUT_PORT, StramElement.OUTPUT_PORT,
        StramElement.ATTR, StramElement.PROP};

    @SuppressWarnings("unused")
    OperatorConf()
    {
    }

    private final Map<String, StreamConf> inputs = Maps.newHashMap();
    private final Map<String, StreamConf> outputs = Maps.newHashMap();
    private String templateRef;

    @Override
    public ConfElement getConfElement()
    {
      return ConfElement.OPERATOR;
    }

    @Override
    public void setProperty(String name, String value)
    {
      if (OPERATOR_TEMPLATE.equals(name)) {
        templateRef = value;
        // Setting opProps from the template as default opProps as before
        // Revisit this
        StramConf stramConf = getAncestorConf(null);
        TemplateConf templateConf = (TemplateConf)stramConf
            .getOrAddChild(value, StramElement.TEMPLATE, elementMaps.get(StramElement.TEMPLATE));
        setDefaultProperties(templateConf.properties);
      } else {
        super.setProperty(name, value);
      }
    }

    private String getClassNameReqd()
    {
      String className = properties.getProperty(OPERATOR_CLASSNAME);
      if (className == null) {
        throw new IllegalArgumentException(String.format("Operator '%s' is missing property '%s'", getId(), LogicalPlanConfiguration.OPERATOR_CLASSNAME));
      }
      return className;
    }

    /**
     * Properties for the node. Template values (if set) become property defaults.
     *
     * @return Map<String,String>
     */
    private Map<String, String> getProperties()
    {
      return Maps.fromProperties(properties);
    }

    @Override
    public StramElement[] getChildElements()
    {
      return CHILD_ELEMENTS;
    }

    @Override
    public StramElement getDefaultChildElement()
    {
      return StramElement.PROP;
    }

    /**
     *
     * @return String
     */
    @Override
    public String toString()
    {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
          .append("id", this.id)
          .toString();
    }

  }

  /**
   * This holds the configuration information for a port on an operator in an Apex application.
   */
  private static class PortConf extends Conf
  {

    private static final StramElement[] CHILD_ELEMENTS = new StramElement[]{StramElement.ATTR, StramElement.UNIFIER};

    @SuppressWarnings("unused")
    PortConf()
    {
    }

    @Override
    public StramElement[] getChildElements()
    {
      return CHILD_ELEMENTS;
    }

    @Override
    public ConfElement getConfElement()
    {
      return ConfElement.PORT;
    }
  }

  private static final Map<StramElement, Class<? extends Conf>> elementMaps = Maps.newHashMap();

  static {
    elementMaps.put(null, StramConf.class);
    elementMaps.put(StramElement.APPLICATION, AppConf.class);
    elementMaps.put(StramElement.GATEWAY, GatewayConf.class);
    elementMaps.put(StramElement.TEMPLATE, TemplateConf.class);
    elementMaps.put(StramElement.OPERATOR, OperatorConf.class);
    elementMaps.put(StramElement.STREAM, StreamConf.class);
    elementMaps.put(StramElement.PORT, PortConf.class);
    elementMaps.put(StramElement.INPUT_PORT, PortConf.class);
    elementMaps.put(StramElement.OUTPUT_PORT, PortConf.class);
    elementMaps.put(StramElement.UNIFIER, OperatorConf.class);
  }

  /**
   * This is a helper method which performs the following checks:<br/><br/>
   * <ol>
   *    <li>If the given {@link StramElement} corresponds to a {@link Conf} type which is
   * the same as the type of the given {@link Conf}, then the given {@link Conf} is returned.</li>
   *    <li>If the given {@link StramElement} corresponds to a {@link Conf} type which is
   * a valid parent {@link Conf} type for the given ancestorConf, then the given ancestor {@link Conf} is
   * returned.</li>
   * @param element The {@link StramElement} type corresponding to this {@link Conf} or
   * to a valid ancestor {@link Conf}.
   * @param ancestorConf The {@link Conf} to return.
   * @return The given {@link Conf}, or null if the first call to this method passes a null {@link StramElement}.
   */
  private static Conf getConf(StramElement element, Conf ancestorConf)
  {
    if (element == ancestorConf.getConfElement().getStramElement()) {
      return ancestorConf;
    }
    // If top most element is reached and didnt match ancestor conf
    // then terminate search
    if (element == null) {
      return null;
    }
    StramElement parentElement = ConfElement.getAllowedParentConf(element);
    Conf parentConf = getConf(parentElement, ancestorConf);

    if (parentConf == null) {
      throw new IllegalArgumentException("The given StramElement is not the same type as the given ancestorConf, " +
                                         "and it is not a valid type for a parent conf.");
    }

    return parentConf.getOrAddChild(WILDCARD, element, elementMaps.get(element));
  }

  /**
   * This method adds a child {@link Conf} with the given {@link StramElement} type and name to the given
   * ancestorConf.
   * @param element The {@link StramElement} of the child {@link Conf} to add to the given ancestorConf.
   * @param name The name of the child {@link Conf} to add to the given ancestorConf.
   * @param ancestorConf The {@link Conf} to add a child {@link Conf} to.
   * @return The child {@link Conf} that was added to the given ancestorConf.
   */
  private static Conf addConf(StramElement element, String name, Conf ancestorConf)
  {
    StramElement parentElement = ConfElement.getAllowedParentConf(element);
    Conf conf1 = null;
    Conf parentConf = getConf(parentElement, ancestorConf);
    if (parentConf != null) {
      conf1 = parentConf.getOrAddChild(name, element, elementMaps.get(element));
    }
    return conf1;
  }

  /**
   * This method returns a list of all the child {@link Conf}s of the given {@link List} of {@link Conf}s with the matching name
   * and {@link StramElement} type.
   * @param <T> The types of the child {@link Conf}s.
   * @param confs The list of {@link Conf}s whose children will be searched.
   * @param name The name of the child {@link Conf}s to return. If the name of the specified child {@link Conf}
   * is null then configurations with the name specified as a {@link LogicalPlanConfiguration#WILDCARD} are matched.
   * @param childType The {@link StramElement} corresponding to the type of a child {@link Conf}.
   * @return The list of child {@link Conf}s with a matching name and {@link StramElement} type.
   */
  private <T extends Conf> List<T> getMatchingChildConf(List<? extends Conf> confs, String name, StramElement childType)
  {
    List<T> childConfs = Lists.newArrayList();
    for (Conf conf1 : confs) {
      List<T> matchingConfs = conf1.getMatchingChildConf(name, childType);
      childConfs.addAll(matchingConfs);
    }
    return childConfs;
  }

  private final Properties properties = new Properties();
  public final Configuration conf;

  private final StramConf stramConf = new StramConf();

  public LogicalPlanConfiguration(Configuration conf)
  {
    this.conf = conf;
    this.addFromConfiguration(conf);
    this.pluginManager = DAGSetupPluginManager.getInstance(conf);
  }

  /**
   * Add operators from flattened name value pairs in configuration object.
   * @param conf
   */
  public final void addFromConfiguration(Configuration conf)
  {
    addFromProperties(toProperties(conf), null);
  }

  private static Properties toProperties(Configuration conf)
  {
    Iterator<Entry<String, String>> it = conf.iterator();
    Properties props = new Properties();
    while (it.hasNext()) {
      Entry<String, String> e = it.next();
      props.put(e.getKey(), e.getValue());
    }
    return props;
  }

  /**
   * Get the application alias name for an application class if one is available.
   * The path for the application class is specified as a parameter. If an alias was specified
   * in the configuration file or configuration opProps for the application class it is returned
   * otherwise null is returned.
   *
   * @param appPath The path of the application class in the jar
   * @return The alias name if one is available, null otherwise
   */
  public String getAppAlias(String appPath)
  {
    String appAlias;
    if (appPath.endsWith(CLASS_SUFFIX)) {
      appPath = appPath.replace("/", KEY_SEPARATOR).substring(0, appPath.length() - CLASS_SUFFIX.length());
    }
    appAlias = stramConf.appAliases.get(appPath);
    if (appAlias == null) {
      try {
        ApplicationAnnotation an = Thread.currentThread().getContextClassLoader().loadClass(appPath).getAnnotation(ApplicationAnnotation.class);
        if (an != null && StringUtils.isNotBlank(an.name())) {
          appAlias = an.name();
        }
      } catch (ClassNotFoundException e) {
        // ignore
      }
    }
    return appAlias;
  }

  public LogicalPlanConfiguration addFromJson(JSONObject json, Configuration conf) throws JSONException
  {
    Properties prop = new Properties();
    JSONArray operatorArray = json.getJSONArray("operators");
    for (int i = 0; i < operatorArray.length(); i++) {
      JSONObject operator = operatorArray.getJSONObject(i);
      String operatorPrefix = StreamingApplication.APEX_PREFIX + StramElement.OPERATOR.getValue() + KEY_SEPARATOR + operator.getString("name") + ".";
      prop.setProperty(operatorPrefix + "classname", operator.getString("class"));
      JSONObject operatorProperties = operator.optJSONObject("properties");
      if (operatorProperties != null) {
        String propertiesPrefix = operatorPrefix + StramElement.PROP.getValue() + KEY_SEPARATOR;
        @SuppressWarnings("unchecked")
        Iterator<String> iter = operatorProperties.keys();
        while (iter.hasNext()) {
          String key = iter.next();
          prop.setProperty(propertiesPrefix + key, operatorProperties.get(key).toString());
        }
      }
      JSONObject operatorAttributes = operator.optJSONObject("attributes");
      if (operatorAttributes != null) {
        String attributesPrefix = operatorPrefix + StramElement.ATTR.getValue() + KEY_SEPARATOR;
        @SuppressWarnings("unchecked")
        Iterator<String> iter = operatorAttributes.keys();
        while (iter.hasNext()) {
          String key = iter.next();
          prop.setProperty(attributesPrefix + key, operatorAttributes.getString(key));
        }
      }
      JSONArray portArray = operator.optJSONArray("ports");
      if (portArray != null) {
        String portsPrefix = operatorPrefix + StramElement.PORT.getValue() + KEY_SEPARATOR;
        for (int j = 0; j < portArray.length(); j++) {
          JSONObject port = portArray.getJSONObject(j);
          JSONObject portAttributes = port.optJSONObject("attributes");
          if (portAttributes != null) {
            String portAttributePrefix = portsPrefix + port.getString("name") + KEY_SEPARATOR + StramElement.ATTR.getValue() + KEY_SEPARATOR;
            @SuppressWarnings("unchecked")
            Iterator<String> iter = portAttributes.keys();
            while (iter.hasNext()) {
              String key = iter.next();
              prop.setProperty(portAttributePrefix + key, portAttributes.getString(key));
            }
          }
        }
      }
    }

    JSONObject appAttributes = json.optJSONObject("attributes");
    if (appAttributes != null) {
      String attributesPrefix = StreamingApplication.APEX_PREFIX + StramElement.ATTR.getValue() + KEY_SEPARATOR;
      @SuppressWarnings("unchecked")
      Iterator<String> iter = appAttributes.keys();
      while (iter.hasNext()) {
        String key = iter.next();
        prop.setProperty(attributesPrefix + key, appAttributes.getString(key));
      }
    }

    JSONArray streamArray = json.getJSONArray("streams");
    for (int i = 0; i < streamArray.length(); i++) {
      JSONObject stream = streamArray.getJSONObject(i);
      String name = stream.optString("name", "stream-" + i);
      String streamPrefix = StreamingApplication.APEX_PREFIX + StramElement.STREAM.getValue() + KEY_SEPARATOR + name + KEY_SEPARATOR;
      JSONObject source = stream.getJSONObject("source");
      prop.setProperty(streamPrefix + STREAM_SOURCE, source.getString("operatorName") + KEY_SEPARATOR + source.getString("portName"));
      JSONArray sinks = stream.getJSONArray("sinks");
      StringBuilder sinkPropertyValue = new StringBuilder();
      for (int j = 0; j < sinks.length(); j++) {
        if (sinkPropertyValue.length() > 0) {
          sinkPropertyValue.append(",");
        }
        JSONObject sink = sinks.getJSONObject(j);
        sinkPropertyValue.append(sink.getString("operatorName")).append(KEY_SEPARATOR).append(sink.getString("portName"));
      }
      prop.setProperty(streamPrefix + STREAM_SINKS, sinkPropertyValue.toString());
      String locality = stream.optString("locality", null);
      if (locality != null) {
        prop.setProperty(streamPrefix + STREAM_LOCALITY, locality);
      }
      JSONObject schema = stream.optJSONObject("schema");
      if (schema != null) {
        String schemaClass = schema.getString("class");
        prop.setProperty(streamPrefix + STREAM_SCHEMA, schemaClass);
      }
    }
    return addFromProperties(prop, conf);
  }


  /**
   * Read operator configurations from properties. The properties can be in any
   * random order, as long as they represent a consistent configuration in their
   * entirety.
   *
   * @param props
   * @param conf configuration for variable substitution and evaluation
   * @return Logical plan configuration.
   */
  public LogicalPlanConfiguration addFromProperties(Properties props, Configuration conf)
  {
    if (conf != null) {
      StramClientUtils.evalProperties(props, conf);
    }
    for (final String propertyName : props.stringPropertyNames()) {
      String propertyValue = props.getProperty(propertyName);
      this.properties.setProperty(propertyName, propertyValue);
      if (propertyName.startsWith(StreamingApplication.DT_PREFIX) ||
          propertyName.startsWith(StreamingApplication.APEX_PREFIX)) {
        String[] keyComps = propertyName.split(KEY_SEPARATOR_SPLIT_REGEX);
        parseStramPropertyTokens(keyComps, 1, propertyName, propertyValue, stramConf);
      }
    }
    return this;
  }

  /**
   * This method is used to parse an Apex property name.
   * @param keys The keys into which an Apex property is split into.
   * @param index The current index that the parser is on for processing the property name.
   * @param propertyName The original unsplit Apex property name.
   * @param propertyValue The value corresponding to the Apex property.
   * @param conf The current {@link Conf} to add properties to.
   */
  private void parseStramPropertyTokens(String[] keys, int index, String propertyName, String propertyValue, Conf conf)
  {
    if (index < keys.length) {
      String key = keys[index];
      StramElement element = getElement(key, conf);
      if ((element == null) && conf.ignoreUnknownChildren()) {
        return;
      }
      if ((element == StramElement.APPLICATION) || (element == StramElement.OPERATOR) || (element == StramElement.STREAM)
          || (element == StramElement.PORT) || (element == StramElement.INPUT_PORT) || (element == StramElement.OUTPUT_PORT)
          || (element == StramElement.TEMPLATE)) {
        parseAppElement(index, keys, element, conf, propertyName, propertyValue);
      } else if (element == StramElement.GATEWAY) {
        parseGatewayElement(element, conf, keys, index, propertyName, propertyValue);
      } else if ((element == StramElement.UNIFIER)) {
        parseUnifierElement(element, conf, keys, index, propertyName, propertyValue);
      } else if ((element == StramElement.ATTR) || ((element == null) && (conf.getDefaultChildElement() == StramElement.ATTR))) {
        parseAttributeElement(element, keys, index, conf, propertyValue, propertyName);
      } else if ((element == StramElement.PROP) || ((element == null) && (conf.getDefaultChildElement() == StramElement.PROP))) {
        parsePropertyElement(element, keys, index, conf, propertyValue, propertyName);
      } else if (element != null) {
        conf.parseElement(element, keys, index, propertyValue);
      }
    }
  }

  /**
   * This is a helper method for {@link #parseStramPropertyTokens} which is responsible for parsing an app element.
   * @param element The current {@link StramElement} of the property being parsed.
   * @param keys The keys that the property being parsed was split into.
   * @param index The current key that the parser is on.
   * @param propertyValue The value associated with the property being parsed.
   * @param propertyName The complete unprocessed name of the property being parsed.
   */
  private void parseAppElement(int index, String[] keys, StramElement element, Conf conf1, String propertyName, String propertyValue)
  {
    if ((index + 1) < keys.length) {
      String name = keys[index + 1];
      Conf elConf = addConf(element, name, conf1);
      if (elConf != null) {
        parseStramPropertyTokens(keys, index + 2, propertyName, propertyValue, elConf);
      } else {
        LOG.error("Invalid configuration key: {}", propertyName);
      }
    } else {
      LOG.warn("Invalid configuration key: {}", propertyName);
    }
  }

  /**
   * This is a helper method for {@link #parseStramPropertyTokens} which is responsible for parsing a gateway element.
   * @param element The current {@link StramElement} of the property being parsed.
   * @param keys The keys that the property being parsed was split into.
   * @param index The current key that the parser is on.
   * @param propertyValue The value associated with the property being parsed.
   * @param propertyName The complete unprocessed name of the property being parsed.
   */
  private void parseGatewayElement(StramElement element, Conf conf1, String[] keys, int index, String propertyName, String propertyValue)
  {
    Conf elConf = addConf(element, null, conf1);
    if (elConf != null) {
      parseStramPropertyTokens(keys, index + 1, propertyName, propertyValue, elConf);
    } else {
      LOG.error("Invalid configuration key: {}", propertyName);
    }
  }

  /**
   * This is a helper method for {@link #parseStramPropertyTokens} which is responsible for parsing a unifier element.
   * @param element The current {@link StramElement} of the property being parsed.
   * @param keys The keys that the property being parsed was split into.
   * @param index The current key that the parser is on.
   * @param propertyValue The value associated with the property being parsed.
   * @param propertyName The complete unprocessed name of the property being parsed.
   */
  private void parseUnifierElement(StramElement element, Conf conf1, String[] keys, int index, String propertyName, String propertyValue)
  {
    Conf elConf = addConf(element, null, conf1);
    if (elConf != null) {
      parseStramPropertyTokens(keys, index + 1, propertyName, propertyValue, elConf);
    } else {
      LOG.error("Invalid configuration key: {}", propertyName);
    }
  }

  /**
   * This is a helper method for {@link #parseStramPropertyTokens} which is responsible for parsing an attribute.
   * @param element The current {@link StramElement} of the property being parsed.
   * @param keys The keys that the property being parsed was split into.
   * @param index The current key that the parser is on.
   * @param conf The current {@link Conf}.
   * @param propertyValue The value associated with the property being parsed.
   * @param propertyName The complete unprocessed name of the property being parsed.
   */
  private void parseAttributeElement(StramElement element, String[] keys, int index, Conf conf, String propertyValue, String propertyName)
  {
    String attributeName = AttributeParseUtils.getAttributeName(element, keys, index);
    if (element != StramElement.ATTR) {
      String expName = getCompleteKey(keys, 0, index) + KEY_SEPARATOR + StramElement.ATTR.getValue() + KEY_SEPARATOR + attributeName;
      LOG.warn("Referencing the attribute as {} instead of {} is deprecated!", getCompleteKey(keys, 0), expName);
    }
    if (conf.getConfElement().getStramElement() == null) {
      conf = addConf(StramElement.APPLICATION, WILDCARD, conf);
    }
    if (conf != null) {
      if (AttributeParseUtils.isSimpleAttributeName(attributeName)) {
        //The provided attribute name was a simple name
        if (!AttributeParseUtils.ALL_SIMPLE_ATTRIBUTE_NAMES.contains(attributeName)) {
          throw new ValidationException("Invalid attribute reference: " + getCompleteKey(keys, 0));
        }
        if (!conf.getConfElement().getAllChildAttributes().contains(attributeName)) {
          throw new ValidationException(attributeName + " is not defined for the " + conf.getConfElement().getStramElement() + " or any of its child configurations.");
        }
        if (conf.getConfElement().getAmbiguousAttributes().contains(attributeName)) {
          //If the attribute name is ambiguous at this configuration level we should tell the user.
          LOG.warn("The attribute " + attributeName + " is ambiguous when specified on an " + conf.getConfElement().getStramElement());
        }
        if (conf.getConfElement().getContextAttributes().contains(attributeName)) {
          @SuppressWarnings(value = "unchecked")
          Attribute<Object> attr = (Attribute<Object>)ContextUtils.CONTEXT_TO_ATTRIBUTE_NAME_TO_ATTRIBUTE.get(conf.getConfElement().getContextClass()).get(attributeName);
          conf.setAttribute(attr, propertyValue);
        } else {
          AttributeParseUtils.processAllConfsForAttribute(conf, attributeName, propertyValue);
        }
      } else {
        //This is a FQ attribute name
        Class<? extends Context> contextClass = AttributeParseUtils.getContainingContextClass(attributeName);
        //Convert to a simple name
        attributeName = AttributeParseUtils.getSimpleAttributeName(attributeName);
        if (!ContextUtils.CONTEXT_CLASS_TO_ATTRIBUTES.get(contextClass).contains(attributeName)) {
          throw new ValidationException(attributeName + " is not a valid attribute in " + contextClass.getCanonicalName());
        }
        ConfElement confWithAttr = ConfElement.CONTEXT_TO_CONF_ELEMENT.get(contextClass);
        conf = ConfElement.addConfs(conf, confWithAttr);
        @SuppressWarnings("unchecked")
        Attribute<Object> attr = (Attribute<Object>)ContextUtils.CONTEXT_TO_ATTRIBUTE_NAME_TO_ATTRIBUTE.get(confWithAttr.getContextClass()).get(attributeName);
        conf.setAttribute(attr, propertyValue);
      }
    } else {
      LOG.error("Invalid configuration key: {}", propertyName);
    }
  }

  /**
   * This is a helper method for {@link #parseStramPropertyTokens} which is responsible for parsing a prop.
   * @param element The current {@link StramElement} of the property being parsed.
   * @param keys The keys that the property being parsed was split into.
   * @param index The current key that the parser is on.
   * @param conf The current {@link Conf}.
   * @param propertyValue The value associated with the property being parsed.
   * @param propertyName The complete unprocessed name of the property being parsed.
   */
  private void parsePropertyElement(StramElement element, String[] keys, int index, Conf conf, String propertyValue, String propertyName)
  {
    // Currently opProps are only supported on operators and streams
    // Supporting current implementation where property can be directly specified under operator
    String prop;
    if (element == StramElement.PROP) {
      prop = getCompleteKey(keys, index + 1);
    } else {
      prop = getCompleteKey(keys, index);
    }
    if (prop != null) {
      conf.setProperty(prop, propertyValue);
    } else {
      LOG.warn("Invalid property specification, no property name specified for {}", propertyName);
    }
  }

  private StramElement getElement(String value, Conf conf)
  {
    StramElement element = null;
    try {
      element = StramElement.fromValue(value);
    } catch (IllegalArgumentException ie) {
      // fall through
    }
    // If element is not allowed treat it as text
    if ((element != null) && !conf.isAllowedChild(element)) {
      element = null;
    }
    return element;
  }

  /**
   * This constructs a string from the keys in the given keys array starting from
   * the start index inclusive until the end of the array.
   * @param keys The keys from which to construct a string.
   * @param start The token to start creating a string from.
   * @return The completed key.
   */
  private static String getCompleteKey(String[] keys, int start)
  {
    return getCompleteKey(keys, start, keys.length);
  }

  /**
   * This constructs a string from the keys in the given keys array starting from
   * the start index inclusive until the specified end index exclusive.
   * @param keys The keys from which to construct a string.
   * @param start The token to start creating a string from.
   * @param end 1 + the last index to include in the concatenation.
   * @return The completed key.
   */
  private static String getCompleteKey(String[] keys, int start, int end)
  {
    int length = 0;
    for (int keyIndex = 0; keyIndex < keys.length; keyIndex++) {
      length += keys[keyIndex].length();
    }

    StringBuilder sb = new StringBuilder(length);
    for (int i = start; i < end; ++i) {
      if (i > start) {
        sb.append(KEY_SEPARATOR);
      }
      sb.append(keys[i]);
    }
    return sb.toString();
  }

  /**
   * Return all opProps set on the builder.
   * Can be serialized to property file and used to read back into builder.
   * @return Properties
   */
  public Properties getProperties()
  {
    return this.properties;
  }

  public Map<String, String> getAppAliases()
  {
    return Collections.unmodifiableMap(this.stramConf.appAliases);
  }

  private LogicalPlan populateDAGAndValidate(LogicalPlanConfiguration tb, String appName)
  {
    LogicalPlan dag = new LogicalPlan();
    pluginManager.setup(dag);
    pluginManager.dispatch(PRE_POPULATE_DAG.event);
    tb.populateDAG(dag);
    // configure with embedded settings
    tb.prepareDAG(dag, null, appName);
    pluginManager.dispatch(POST_POPULATE_DAG.event);
    // configure with external settings
    prepareDAG(dag, null, appName);
    pluginManager.dispatch(PRE_VALIDATE_DAG.event);
    dag.validate();
    pluginManager.dispatch(POST_VALIDATE_DAG.event);
    pluginManager.teardown();
    return dag;
  }

  public LogicalPlan createFromProperties(Properties props, String appName) throws IOException
  {
    // build DAG from properties
    LogicalPlanConfiguration tb = new LogicalPlanConfiguration(new Configuration(false));
    tb.addFromProperties(props, conf);
    return populateDAGAndValidate(tb, appName);
  }

  public LogicalPlan createFromJson(JSONObject json, String appName) throws Exception
  {
    // build DAG from properties
    LogicalPlanConfiguration tb = new LogicalPlanConfiguration(new Configuration(false));
    tb.addFromJson(json, conf);
    return populateDAGAndValidate(tb, appName);
  }

  public LogicalPlan createEmptyForRecovery(String appName)
  {
    // build DAG from properties
    LogicalPlanConfiguration tb = new LogicalPlanConfiguration(new Configuration(false));
    return populateDAGAndValidate(tb, appName);
  }

  public LogicalPlan createFromStreamingApplication(StreamingApplication app, String appName)
  {
    LogicalPlan dag = new LogicalPlan();
    pluginManager.setup(dag);
    prepareDAG(dag, app, appName);
    pluginManager.dispatch(PRE_VALIDATE_DAG.event);
    dag.validate();
    pluginManager.dispatch(POST_VALIDATE_DAG.event);
    pluginManager.teardown();
    return dag;
  }

  /**
   * Populate the logical plan structure from properties.
   * @param dag
   */
  public void populateDAG(LogicalPlan dag)
  {
    Configuration pconf = new Configuration(conf);
    for (final String propertyName : this.properties.stringPropertyNames()) {
      String propertyValue = this.properties.getProperty(propertyName);
      pconf.setIfUnset(propertyName, propertyValue);
    }

    AppConf appConf = this.stramConf.getChild(WILDCARD, StramElement.APPLICATION);
    if (appConf == null) {
      LOG.warn("Application configuration not found. Probably an empty app.");
      return;
    }

    Map<String, OperatorConf> operators = appConf.getChildren(StramElement.OPERATOR);

    Map<OperatorConf, GenericOperator> nodeMap = Maps.newHashMapWithExpectedSize(operators.size());
    // add all operators first
    for (Map.Entry<String, OperatorConf> nodeConfEntry : operators.entrySet()) {
      OperatorConf nodeConf = nodeConfEntry.getValue();
      if (!WILDCARD.equals(nodeConf.id)) {
        Class<? extends GenericOperator> nodeClass = StramUtils.classForName(nodeConf.getClassNameReqd(), GenericOperator.class);
        String optJson = nodeConf.getProperties().get(nodeClass.getName());
        GenericOperator operator = null;
        try {
          if (optJson != null) {
            // if there is a special key which is the class name, it means the operator is serialized in json format
            ObjectMapper mapper = ObjectMapperFactory.getOperatorValueDeserializer();
            operator = mapper.readValue("{\"" + nodeClass.getName() + "\":" + optJson + "}", nodeClass);
            addOperator(dag, nodeConfEntry.getKey(), operator);
          } else {
            operator = addOperator(dag, nodeConfEntry.getKey(), nodeClass);
          }
          setOperatorProperties(operator, nodeConf.getProperties());
        } catch (IOException e) {
          throw new IllegalArgumentException("Error setting operator properties " + e.getMessage(), e);
        }
        nodeMap.put(nodeConf, operator);
      }
    }

    Map<String, StreamConf> streams = appConf.getChildren(StramElement.STREAM);

    // wire operators
    for (Map.Entry<String, StreamConf> streamConfEntry : streams.entrySet()) {
      StreamConf streamConf = streamConfEntry.getValue();
      DAG.StreamMeta sd = dag.addStream(streamConfEntry.getKey());
      sd.setLocality(streamConf.getLocality());

      String schemaClassName = streamConf.properties.getProperty(STREAM_SCHEMA);
      Class<?> schemaClass = null;
      if (schemaClassName != null) {
        schemaClass = StramUtils.classForName(schemaClassName, Object.class);
      }

      if (streamConf.sourceNode != null) {
        String portName = null;
        for (Map.Entry<String, StreamConf> e : streamConf.sourceNode.outputs.entrySet()) {
          if (e.getValue() == streamConf) {
            portName = e.getKey();
          }
        }
        GenericOperator sourceDecl = nodeMap.get(streamConf.sourceNode);
        Operators.PortMappingDescriptor sourcePortMap = new Operators.PortMappingDescriptor();
        Operators.describe(sourceDecl, sourcePortMap);
        sd.setSource(sourcePortMap.outputPorts.get(portName).component);

        if (schemaClass != null) {
          dag.setOutputPortAttribute(sourcePortMap.outputPorts.get(portName).component, PortContext.TUPLE_CLASS, schemaClass);
        }
      }

      for (OperatorConf targetNode : streamConf.targetNodes) {
        String portName = null;
        for (Map.Entry<String, StreamConf> e : targetNode.inputs.entrySet()) {
          if (e.getValue() == streamConf) {
            portName = e.getKey();
          }
        }
        GenericOperator targetDecl = nodeMap.get(targetNode);
        Operators.PortMappingDescriptor targetPortMap = new Operators.PortMappingDescriptor();
        Operators.describe(targetDecl, targetPortMap);
        sd.addSink(targetPortMap.inputPorts.get(portName).component);

        if (schemaClass != null) {
          dag.setInputPortAttribute(targetPortMap.inputPorts.get(portName).component, PortContext.TUPLE_CLASS, schemaClass);
        }
      }
    }
  }

  private GenericOperator addOperator(LogicalPlan dag, String name, GenericOperator operator)
  {
    if (operator instanceof Module) {
      dag.addModule(name, (Module)operator);
    } else if (operator instanceof Operator) {
      dag.addOperator(name, (Operator)operator);
    }
    return operator;
  }


  private GenericOperator addOperator(LogicalPlan dag, String name, Class<?> clazz)
  {
    if (Module.class.isAssignableFrom(clazz)) {
      return dag.addModule(name, (Class<Module>)clazz);
    } else if (Operator.class.isAssignableFrom(clazz)) {
      return dag.addOperator(name, (Class<Operator>)clazz);
    }
    return null;
  }

  /**
   * Populate the logical plan from the streaming application definition and configuration.
   * Configuration is resolved based on application alias, if any.
   * @param app The {@link StreamingApplication} to be run.
   * @param dag This will hold the {@link LogicalPlan} representation of the given {@link StreamingApplication}.
   * @param name The path of the application class in the jar.
   */
  public void prepareDAG(LogicalPlan dag, StreamingApplication app, String name)
  {
    prepareDAGAttributes(dag);

    pluginManager.setup(dag);
    if (app != null) {
      pluginManager.dispatch(PRE_POPULATE_DAG.event);
      app.populateDAG(dag, conf);
      pluginManager.dispatch(POST_POPULATE_DAG.event);
    }
    pluginManager.dispatch(PRE_CONFIGURE_DAG.event);
    String appAlias = getAppAlias(name);
    String appName = appAlias == null ? name : appAlias;
    List<AppConf> appConfs = stramConf.getMatchingChildConf(appName, StramElement.APPLICATION);
    setApplicationConfiguration(dag, appConfs, app);
    if (dag.getAttributes().get(Context.DAGContext.APPLICATION_NAME) == null) {
      dag.setAttribute(Context.DAGContext.APPLICATION_NAME, appName);
    }

    // Expand the modules within the dag recursively
    setModuleProperties(dag, appName);
    flattenDAG(dag, conf);

    // inject external operator configuration
    setOperatorConfiguration(dag, appConfs, appName);
    setStreamConfiguration(dag, appConfs, appName);
    pluginManager.dispatch(POST_CONFIGURE_DAG.event);
    pluginManager.teardown();
  }

  private void prepareDAGAttributes(LogicalPlan dag)
  {
    // Consider making all attributes available for DAG construction
    // EVENTUALLY to be replaced by variable enabled configuration in the demo where the attribute below is used
    String connectAddress = conf.get(KEY_GATEWAY_CONNECT_ADDRESS);
    dag.setAttribute(DAGContext.GATEWAY_CONNECT_ADDRESS, connectAddress == null ? conf.get(GATEWAY_LISTEN_ADDRESS) : connectAddress);
    if (conf.getBoolean(KEY_GATEWAY_USE_SSL, DAGContext.GATEWAY_USE_SSL.defaultValue)) {
      dag.setAttribute(DAGContext.GATEWAY_USE_SSL, true);
    }
    String username = conf.get(KEY_GATEWAY_USER_NAME);
    if (username != null) {
      dag.setAttribute(DAGContext.GATEWAY_USER_NAME, username);
    }
    String password = conf.get(KEY_GATEWAY_PASSWORD);
    if (password != null) {
      dag.setAttribute(DAGContext.GATEWAY_PASSWORD, password);
    }
  }

  private void flattenDAG(LogicalPlan dag, Configuration conf)
  {
    for (ModuleMeta moduleMeta : dag.getAllModules()) {
      moduleMeta.flattenModule(dag, conf);
    }
    dag.applyStreamLinks();
  }

  public static Properties readProperties(String filePath) throws IOException
  {
    InputStream is = new FileInputStream(filePath);
    Properties props = new Properties(System.getProperties());
    props.load(is);
    is.close();
    return props;
  }

  /**
   * Get the configuration opProps for the given operator.
   * These can be operator specific settings or settings from matching templates.
   * @param ow
   * @param appName
   * @return
   */
  public Map<String, String> getProperties(OperatorMeta ow, String appName)
  {
    List<AppConf> appConfs = stramConf.getMatchingChildConf(appName, StramElement.APPLICATION);
    List<OperatorConf> opConfs = getMatchingChildConf(appConfs, ow.getName(), StramElement.OPERATOR);
    return getProperties(getPropertyArgs(ow), opConfs, appName);
  }

  private Map<String, String> getApplicationProperties(List<AppConf> appConfs)
  {
    Map<String, String> appProps = Maps.newHashMap();
    // Apply the configurations in reverse order since the higher priority ones are at the beginning
    for (int i = appConfs.size() - 1; i >= 0; i--) {
      AppConf conf1 = appConfs.get(i);
      appProps.putAll(Maps.fromProperties(conf1.properties));
    }
    return appProps;
  }

  /**
   * Get the configuration opProps for the given operator.
   * These can be operator specific settings or settings from matching templates.
   * @param pa
   * @param opConfs
   * @param appName
   */
  private Map<String, String> getProperties(PropertyArgs pa, List<OperatorConf> opConfs, String appName)
  {
    Map<String, String> opProps = Maps.newHashMap();
    Map<String, TemplateConf> templates = stramConf.getChildren(StramElement.TEMPLATE);
    // list of all templates that match operator, ordered by priority
    if (!templates.isEmpty()) {
      TreeMap<Integer, TemplateConf> matchingTemplates = getMatchingTemplates(pa, appName, templates);
      if (matchingTemplates != null && !matchingTemplates.isEmpty()) {
        // combined map of prioritized template settings
        for (TemplateConf t : matchingTemplates.descendingMap().values()) {
          opProps.putAll(Maps.fromProperties(t.properties));
        }
      }

      List<TemplateConf> refTemplates = getDirectTemplates(opConfs, templates);
      for (TemplateConf t : refTemplates) {
        opProps.putAll(Maps.fromProperties(t.properties));
      }
    }
    // direct settings
    // Apply the configurations in reverse order since the higher priority ones are at the beginning
    for (int i = opConfs.size() - 1; i >= 0; i--) {
      Conf conf1 = opConfs.get(i);
      opProps.putAll(Maps.fromProperties(conf1.properties));
    }
    return opProps;
  }

  private List<TemplateConf> getDirectTemplates(List<OperatorConf> opConfs, Map<String, TemplateConf> templates)
  {
    List<TemplateConf> refTemplates = Lists.newArrayList();
    for (TemplateConf t : templates.values()) {
      for (OperatorConf opConf : opConfs) {
        if (t.id.equals(opConf.templateRef)) {
          refTemplates.add(t);
        }
      }
    }
    return refTemplates;
  }

  private static class PropertyArgs
  {
    String name;
    String className;

    public PropertyArgs(String name, String className)
    {
      this.name = name;
      this.className = className;
    }
  }

  private PropertyArgs getPropertyArgs(OperatorMeta om)
  {
    return new PropertyArgs(om.getName(), om.getGenericOperator().getClass().getName());
  }

  /**
   * Produce the collections of templates that apply for the given id.
   * @param pa
   * @param appName
   * @param templates
   * @return TreeMap<Integer, TemplateConf>
   */
  private TreeMap<Integer, TemplateConf> getMatchingTemplates(PropertyArgs pa, String appName, Map<String, TemplateConf> templates)
  {
    TreeMap<Integer, TemplateConf> tm = Maps.newTreeMap();
    for (TemplateConf t : templates.values()) {
      if ((t.idRegExp != null && pa.name.matches(t.idRegExp))) {
        tm.put(1, t);
      } else if (appName != null && t.appNameRegExp != null
          && appName.matches(t.appNameRegExp)) {
        tm.put(2, t);
      } else if (t.classNameRegExp != null
          && pa.className.matches(t.classNameRegExp)) {
        tm.put(3, t);
      }
    }
    return tm;
  }

  /**
   * Inject the configuration opProps into the operator instance.
   * @param operator
   * @param properties
   * @return Operator
   */
  public static GenericOperator setOperatorProperties(GenericOperator operator, Map<String, String> properties)
  {
    try {
      // populate custom opProps
      BeanUtils.populate(operator, properties);
      return operator;
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalArgumentException("Error setting operator properties", e);
    }
  }

  public static StreamingApplication setApplicationProperties(StreamingApplication application, Map<String, String> properties)
  {
    try {
      BeanUtils.populate(application, properties);
      return application;
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalArgumentException("Error setting application properties", e);
    }
  }

  public static BeanMap getObjectProperties(Object obj)
  {
    return new BeanMap(obj);
  }

  /**
   * Set any opProps from configuration on the operators in the DAG. This
   * method may throw unchecked exception if the configuration contains
   * opProps that are invalid for an operator.
   *
   * @param dag
   * @param applicationName
   */
  public void setOperatorProperties(LogicalPlan dag, String applicationName)
  {
    List<AppConf> appConfs = stramConf.getMatchingChildConf(applicationName, StramElement.APPLICATION);
    for (OperatorMeta ow : dag.getAllOperators()) {
      List<OperatorConf> opConfs = getMatchingChildConf(appConfs, ow.getName(), StramElement.OPERATOR);
      Map<String, String> opProps = getProperties(getPropertyArgs(ow), opConfs, applicationName);
      setOperatorProperties(ow.getGenericOperator(), opProps);
    }
  }

  /**
   * Set any properties from configuration on the modules in the DAG. This
   * method may throw unchecked exception if the configuration contains
   * properties that are invalid for a module.
   *
   * @param dag
   * @param applicationName
   */
  public void setModuleProperties(LogicalPlan dag, String applicationName)
  {
    List<AppConf> appConfs = stramConf.getMatchingChildConf(applicationName, StramElement.APPLICATION);
    setModuleConfiguration(dag, appConfs, applicationName);
  }

  /**
   * Set the application configuration.
   * @param dag
   * @param appName
   * @param app
   */
  public void setApplicationConfiguration(final LogicalPlan dag, String appName, StreamingApplication app)
  {
    List<AppConf> appConfs = stramConf.getMatchingChildConf(appName, StramElement.APPLICATION);
    setApplicationConfiguration(dag, appConfs, app);
  }

  private void setApplicationConfiguration(final LogicalPlan dag, List<AppConf> appConfs, StreamingApplication app)
  {
    setAttributes(appConfs, dag.getAttributes());
    if (app != null) {
      Map<String, String> appProps = getApplicationProperties(appConfs);
      setApplicationProperties(app, appProps);
    }
  }

  private void setOperatorConfiguration(final LogicalPlan dag, List<AppConf> appConfs, String appName)
  {
    for (final OperatorMeta ow : dag.getAllOperators()) {
      List<OperatorConf> opConfs = getMatchingChildConf(appConfs, ow.getName(), StramElement.OPERATOR);

      // Set the operator attributes
      setAttributes(opConfs, ow.getAttributes());
      // Set the operator opProps
      Map<String, String> opProps = getProperties(getPropertyArgs(ow), opConfs, appName);
      setOperatorProperties(ow.getOperator(), opProps);

      // Set the port attributes
      for (Entry<LogicalPlan.InputPortMeta, LogicalPlan.StreamMeta> entry : ow.getInputStreams().entrySet()) {
        final InputPortMeta im = entry.getKey();
        List<PortConf> inPortConfs = getMatchingChildConf(opConfs, im.getPortName(), StramElement.INPUT_PORT);
        // Add the generic port attributes as well
        List<PortConf> portConfs = getMatchingChildConf(opConfs, im.getPortName(), StramElement.PORT);
        inPortConfs.addAll(portConfs);
        setAttributes(inPortConfs, im.getAttributes());
      }

      for (Entry<LogicalPlan.OutputPortMeta, LogicalPlan.StreamMeta> entry : ow.getOutputStreams().entrySet()) {
        final OutputPortMeta om = entry.getKey();
        List<PortConf> outPortConfs = getMatchingChildConf(opConfs, om.getPortName(), StramElement.OUTPUT_PORT);
        // Add the generic port attributes as well
        List<PortConf> portConfs = getMatchingChildConf(opConfs, om.getPortName(), StramElement.PORT);
        outPortConfs.addAll(portConfs);
        setAttributes(outPortConfs, om.getAttributes());
        List<OperatorConf> unifConfs = getMatchingChildConf(outPortConfs, null, StramElement.UNIFIER);
        if (unifConfs.size() != 0) {
          setAttributes(unifConfs, om.getUnifierMeta().getAttributes());
        }
      }
      ow.populateAggregatorMeta();
    }
  }

  private void setModuleConfiguration(final LogicalPlan dag, List<AppConf> appConfs, String appName)
  {
    for (final ModuleMeta mw : dag.getAllModules()) {
      List<OperatorConf> opConfs = getMatchingChildConf(appConfs, mw.getName(), StramElement.OPERATOR);
      Map<String, String> opProps = getProperties(getPropertyArgs(mw), opConfs, appName);
      setOperatorProperties(mw.getGenericOperator(), opProps);
    }
  }

  private void setStreamConfiguration(LogicalPlan dag, List<AppConf> appConfs, String appAlias)
  {
    for (StreamMeta sm : dag.getAllStreams()) {
      List<StreamConf> smConfs = getMatchingChildConf(appConfs, sm.getName(), StramElement.STREAM);
      for (StreamConf smConf : smConfs) {
        DAG.Locality locality = smConf.getLocality();
        if (locality != null) {
          sm.setLocality(locality);
          break;
        }
      }
    }
  }

  private void setAttributes(List<? extends Conf> confs, Attribute.AttributeMap attributeMap)
  {
    Set<Attribute<Object>> processedAttributes = Sets.newHashSet();
    //json object codec for complex attributes
    JSONObject2String jsonCodec = new JSONObject2String();
    if (confs.size() > 0) {
      for (Conf conf1 : confs) {
        for (Map.Entry<Attribute<Object>, String> e : conf1.attributes.entrySet()) {
          Attribute<Object> attribute = e.getKey();
          if (attribute.codec == null) {
            String msg = String.format("Attribute does not support property configuration: %s %s", attribute.name, e.getValue());
            throw new UnsupportedOperationException(msg);
          } else {
            if (processedAttributes.add(attribute)) {
              String val = e.getValue();
              if (val.trim().charAt(0) == '{' && !(attribute.codec instanceof JsonStringCodec)) {
                // complex attribute in json
                attributeMap.put(attribute, jsonCodec.fromString(val));
              } else {
                attributeMap.put(attribute, attribute.codec.fromString(val));
              }
            }
          }
        }
      }
    }
  }

}
