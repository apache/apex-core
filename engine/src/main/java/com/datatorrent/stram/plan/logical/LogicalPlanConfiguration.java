/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.plan.logical;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import javax.validation.ValidationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.CaseFormat;
import com.google.common.collect.Maps;

import org.apache.commons.beanutils.BeanMap;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.AttributeMap.Attribute;
import com.datatorrent.api.AttributeMap.AttributeInitializer;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAGContext;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;

import com.datatorrent.stram.StramUtils;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OutputPortMeta;
import java.lang.reflect.Constructor;
import java.util.*;

/**
 *
 * Builder for the DAG logical representation of operators and streams from opProps.<p>
 * <br>
 * Supports reading as name-value pairs from Hadoop {@link Configuration} or opProps file.
 * <br>
 *
 * @since 0.3.2
 */
public class LogicalPlanConfiguration implements StreamingApplication {

  private static final Logger LOG = LoggerFactory.getLogger(LogicalPlanConfiguration.class);

  public static final String GATEWAY_ADDRESS = "stram.gateway.address";
  public static final String GATEWAY_ADDRESS_ATTR = "address";

  public static final String STRAM_PREFIX = "stram.";

  public static final String STREAM_PREFIX = "stram.stream";
  public static final String STREAM_SOURCE = "source";
  public static final String STREAM_SINKS = "sinks";
  public static final String STREAM_TEMPLATE = "template";
  public static final String STREAM_LOCALITY = "locality";

  public static final String OPERATOR_PREFIX = "stram.operator";
  public static final String OPERATOR_CLASSNAME = "classname";
  public static final String OPERATOR_TEMPLATE = "template";

  public static final String TEMPLATE_PREFIX = "stram.template";

  public static final String TEMPLATE_idRegExp = "matchIdRegExp";
  public static final String TEMPLATE_appNameRegExp = "matchAppNameRegExp";
  public static final String TEMPLATE_classNameRegExp = "matchClassNameRegExp";

  public static final String APPLICATION_PREFIX = "stram.application";

  public static final String ATTR = "attr";
  public static final String PROP = "prop";
  public static final String CLASS = "class";

  private static final String CLASS_SUFFIX = "." + CLASS;

  private static final String WILDCARD = "*";
  private static final String WILDCARD_PATTERN = ".*";

  private enum StramElement {
    APPLICATION("application"), GATEWAY("gateway"), TEMPLATE("template"), OPERATOR("operator"),STREAM("stream"), PORT("port"), INPUT_PORT("inputport"),OUTPUT_PORT("outputport"),
    ATTR("attr"), PROP("prop"),CLASS("class"),PATH("path");
    private String value;

    StramElement(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    public static StramElement fromValue(String value) {
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

  private static abstract class Conf {

    protected Conf parentConf = null;

    protected final Properties attributes = new Properties();
    protected final PropertiesWithModifiableDefaults properties = new PropertiesWithModifiableDefaults();

    protected Map<StramElement, Map<String, ? extends Conf>> children = Maps.newHashMap();

    protected String id;

    public void setId(String id) {
      this.id = id;
    }

    public String getId() {
      return id;
    }

    public void setParentConf(Conf parentConf) {
      this.parentConf = parentConf;
    }

    public Conf getParentConf() {
      return parentConf;
    }

    public <T extends Conf> T addChild(String id, StramElement childType, Class<T> clazz) {
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

    public void setAttribute(String name, String value) {
      attributes.setProperty(name, value);
    }

    public void setProperty(String name, String value) {
      properties.setProperty(name, value);
    }

    public void setDefaultProperties(Properties defaults) {
      properties.setDefaultProperties(defaults);
    }

    public <T extends Conf> List<T> getMatchingChildConf(String name, StramElement childType) {
      List<T> childConfs = new ArrayList<T>();
      @SuppressWarnings("unchecked")
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
          if (key.equals(WILDCARD)) key = WILDCARD_PATTERN;
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

    protected <T extends Conf> T getOrAddConf(Map<String, T> map, String id, Class<T> clazz) {
      T conf = map.get(id);
      if (conf == null) {
        try {
          Constructor<T> declaredConstructor = clazz.getDeclaredConstructor(new Class<?>[] {});
          conf = declaredConstructor.newInstance(new Object[] {});
          conf.setId(id);
          map.put(id, conf);
        } catch (Exception e) {
          LOG.error("Error instantiating configuration", e);
        }
      }
      return conf;
    }

    //public abstract Conf getChild(String id, StramElement childType);
    public  <T extends Conf> T getChild(String id, StramElement childType) {
      T conf = null;
      @SuppressWarnings("unchecked")
      Map<String, T> elChildren = (Map<String, T>)children.get(childType);
      if (elChildren != null) {
        conf = elChildren.get(id);
      }
      return conf;
    }

    @SuppressWarnings("unchecked")
    public <T extends Conf> Map<String, T> getChildren(StramElement childType) {
      // Always return non null so caller will not have to do extra check as expected
      Map<String, T> elChildren = (Map<String, T>)children.get(childType);
      if (elChildren == null) {
        elChildren = new HashMap<String, T>();
        children.put(childType, elChildren);
      }
      return elChildren;
    }

    // Override for parsing of custom elements other than attributes and opProps
    // Make this config parse element as the entry point for parsing in future instead of the generic method in parent class
    public void parseElement(StramElement element, String[] keys, int index, String propertyValue) {
    }

    public abstract boolean isAllowedElement(StramElement childType);

    public abstract StramElement getElement();

  }

  private static class StramConf extends Conf {

    private final Map<String, String> appAliases = Maps.newHashMap();

    public StramConf() {

    }

    @Override
    public StramElement getElement()
    {
      return null;
    }

    @Override
    public boolean isAllowedElement(StramElement childType)
    {
      return (childType == StramElement.APPLICATION) || (childType == StramElement.GATEWAY) || (childType == StramElement.TEMPLATE)
                   || (childType == StramElement.OPERATOR) || (childType == StramElement.PORT) || (childType == StramElement.INPUT_PORT)
                   || (childType == StramElement.OUTPUT_PORT) || (childType == StramElement.STREAM) || (childType == StramElement.TEMPLATE)
                   || (childType == StramElement.ATTR);
    }

  }

  /**
   * App configuration
   */
  private static class AppConf extends Conf {
    //Properties opProps = new Properties(LogicalPlanConfiguration.this.opProps);

    public AppConf() {
    }

    @Override
    public StramElement getElement()
    {
      return StramElement.APPLICATION;
    }

    @Override
    public void parseElement(StramElement element, String[] keys, int index, String propertyValue) {
      if ((element == StramElement.CLASS) || (element == StramElement.PATH)) {
        StramConf stramConf = (StramConf)getParentConf();
        stramConf.appAliases.put(propertyValue, getId());
      }
    }

    @Override
    public boolean isAllowedElement(StramElement childType)
    {
      return (childType == StramElement.GATEWAY) || (childType == StramElement.OPERATOR)
              || (childType == StramElement.PORT) || (childType == StramElement.INPUT_PORT) || (childType == StramElement.OUTPUT_PORT)
              || (childType == StramElement.STREAM) || (childType == StramElement.ATTR) || (childType == StramElement.CLASS) || (childType == StramElement.PATH);
    }

  }

  private static class GatewayConf extends Conf {

    public GatewayConf() {
    }

    @Override
    public boolean isAllowedElement(StramElement childType)
    {
      return (childType == StramElement.ATTR);
    }

    @Override
    public StramElement getElement()
    {
      return StramElement.GATEWAY;
    }

  }

  /**
   * Named set of opProps that can be used to instantiate streams or operators
   * with common settings.
   */
  private static class TemplateConf extends Conf {
    //private final Properties opProps = new Properties();

    public TemplateConf() {
    }

    /**
     *
     * @param id
     */
    private TemplateConf(String id) {
      this.id = id;
    }

    @Override
    public boolean isAllowedElement(StramElement childType)
    {
      return (childType == StramElement.PROP);
    }

    @Override
    public StramElement getElement()
    {
      return StramElement.TEMPLATE;
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
   *
   */
  private static class StreamConf extends Conf {
    //private final String id;
    private OperatorConf sourceNode;
    private final Set<OperatorConf> targetNodes = new HashSet<OperatorConf>();

    //private final PropertiesWithModifiableDefaults opProps = new PropertiesWithModifiableDefaults();
    //private TemplateConf template;
    private String templateRef;

    public StreamConf() {
    }

    private StreamConf(String id) {
      this.id = id;
    }

    @Override
    public StramElement getElement()
    {
      return StramElement.STREAM;
    }

    /**
     * Locality for adjacent operators.
     * @return boolean
     */
    public DAG.Locality getLocality() {
      String v = properties.getProperty(STREAM_LOCALITY, null);
      return (v != null) ? DAG.Locality.valueOf(v) : null;
    }

    /**
     * Set source on stream to the node output port.
     * @param portName
     * @param node
     */
    public StreamConf setSource(String portName, OperatorConf node) {
      if (this.sourceNode != null) {
        throw new IllegalArgumentException(String.format("Stream already receives input from %s", sourceNode));
      }
      node.outputs.put(portName, this);
      this.sourceNode = node;
      return this;
    }

    public StreamConf addSink(String portName, OperatorConf targetNode) {
      if (targetNode.inputs.containsKey(portName)) {
        throw new IllegalArgumentException(String.format("Port %s already connected to stream %s", portName, targetNode.inputs.get(portName)));
      }
      //LOG.debug("Adding {} to {}", targetNode, this);
      targetNode.inputs.put(portName, this);
      targetNodes.add(targetNode);
      return this;
    }

    @Override
    public void setProperty(String name, String value) {
      AppConf appConf = (AppConf)getParentConf();
      if (STREAM_SOURCE.equals(name)) {
        if (sourceNode != null) {
          // multiple sources not allowed
          //throw new IllegalArgumentException("Duplicate " + propertyName);
          throw new IllegalArgumentException("Duplicate " + name);
        }
        String[] parts = getNodeAndPortId(value);
        //setSource(parts[1], getOrAddOperator(appConf, parts[0]));
        setSource(parts[1], appConf.addChild(parts[0], StramElement.OPERATOR, OperatorConf.class));
      } else if (STREAM_SINKS.equals(name)) {
        String[] targetPorts = value.split(",");
        for (String nodeAndPort : targetPorts) {
          String[] parts = getNodeAndPortId(nodeAndPort.trim());
          addSink(parts[1], appConf.addChild(parts[0], StramElement.OPERATOR, OperatorConf.class));
        }
      } else if (STREAM_TEMPLATE.equals(name)) {
        templateRef = value;
        StramConf stramConf = (StramConf)getParentConf().getParentConf();
        TemplateConf templateConf = (TemplateConf)stramConf.addChild(value, StramElement.TEMPLATE, elementMaps.get(StramElement.TEMPLATE));
        setDefaultProperties(templateConf.properties);
      } else {
        super.setProperty(name, value);
      }
    }

    private String[] getNodeAndPortId(String s) {
      String[] parts = s.split("\\.");
      if (parts.length != 2) {
        throw new IllegalArgumentException("Invalid node.port reference: " + s);
      }
      return parts;
    }

    @Override
    public boolean isAllowedElement(StramElement childType)
    {
      return (childType == StramElement.TEMPLATE) || (childType == StramElement.PROP);
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
          append("id", this.id).
          toString();
    }

  }

  /**
   *
   */
  private static class PropertiesWithModifiableDefaults extends Properties {
    private static final long serialVersionUID = -4675421720308249982L;

    /**
     * @param defaults
     */
    void setDefaultProperties(Properties defaults) {
        super.defaults = defaults;
    }
  }

  /**
   * Operator configuration
   */
  private static class OperatorConf extends Conf {
    OperatorConf() {
    }
    OperatorConf(String id) {
      this.id = id;
    }
    //private final String id;
    /**
     * The opProps of the node, can be subclass opProps which will be set via reflection.
     */
    //private final PropertiesWithModifiableDefaults opProps = new PropertiesWithModifiableDefaults();
    /**
     * The inputs for the node
     */
    private final Map<String, StreamConf> inputs = new HashMap<String, StreamConf>();
    /**
     * The outputs for the node
     */
    private final Map<String, StreamConf> outputs = new HashMap<String, StreamConf>();

    //private TemplateConf template;
    private String templateRef;

    /**
     *
     * @return String
     */
    /*
    public String getId() {
      return id;
    }
    */

    @Override
    public StramElement getElement()
    {
      return StramElement.OPERATOR;
    }

    @Override
    public void setProperty(String name, String value)
    {
      if (OPERATOR_TEMPLATE.equals(name)) {
        templateRef = value;
        // Setting opProps from the template as default opProps as before
        // Revisit this
        StramConf stramConf = (StramConf)getParentConf().getParentConf();
        TemplateConf templateConf = (TemplateConf)stramConf.addChild(value, StramElement.TEMPLATE, elementMaps.get(StramElement.TEMPLATE));
        setDefaultProperties(templateConf.properties);
      } else {
        super.setProperty(name, value);
      }
    }

    private String getClassNameReqd() {
      String className = properties.getProperty(OPERATOR_CLASSNAME);
      if (className == null) {
        throw new IllegalArgumentException(String.format("Operator '%s' is missing property '%s'", getId(), LogicalPlanConfiguration.OPERATOR_CLASSNAME));
      }
      return className;
    }

    /**
     * Properties for the node. Template values (if set) become property defaults.
     * @return Map<String, String>
     */
    private Map<String, String> getProperties() {
      return Maps.fromProperties(properties);
    }

    @Override
    public boolean isAllowedElement(StramElement childType)
    {
      return (childType == StramElement.PORT) || (childType == StramElement.INPUT_PORT) || (childType == StramElement.OUTPUT_PORT)
              /*|| (childType == StramElement.TEMPLATE)*/ || (childType == StramElement.ATTR) || (childType == StramElement.PROP);
    }

    /**
     *
     * @return String
     */
    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).
          append("id", this.id).
          toString();
    }

  }

  /**
   * Port configuration
   */
  private static class PortConf extends Conf {

    public PortConf() {
    }

    @Override
    public StramElement getElement()
    {
      return StramElement.PORT;
    }

    @Override
    public boolean isAllowedElement(StramElement childType)
    {
      return (childType == StramElement.ATTR);
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
  }

  private Conf getConf(StramElement element, Conf ancestorConf) {
    if (element == ancestorConf.getElement()) {
      return ancestorConf;
    }
    StramElement parentElement = getAllowedParentElement(element, ancestorConf);
    Conf parentConf = getConf(parentElement, ancestorConf);
    Conf conf = parentConf.addChild(WILDCARD, element, elementMaps.get(element));
    return conf;
  }

  private Conf addConf(StramElement element, String name, Conf ancestorConf) {
    StramElement parentElement = getAllowedParentElement(element, ancestorConf);
    Conf parentConf = getConf(parentElement, ancestorConf);
    Conf conf = parentConf.addChild(name, element, elementMaps.get(element) );
    return conf;
  }

  private StramElement getAllowedParentElement(StramElement element, Conf ancestorConf) {
    StramElement parentElement = null;
    if (element == StramElement.APPLICATION) {
      parentElement = null;
    } else if ((element == StramElement.GATEWAY) || (element == StramElement.OPERATOR) || (element == StramElement.STREAM)) {
      parentElement = StramElement.APPLICATION;
    } else if ((element == StramElement.PORT) || (element == StramElement.INPUT_PORT) || (element == StramElement.OUTPUT_PORT)) {
      parentElement = StramElement.OPERATOR;
    } else if (element == StramElement.TEMPLATE) {
      parentElement = null;
    }
    return parentElement;
  }

  /*
  private boolean isApplicationTypeConf(Conf conf) {
    return (conf.getElement() == null) || (conf.getElement() == StramElement.APPLICATION);
  }
  */

  private <T extends Conf> List<T> getMatchingChildConf(List<? extends Conf> confs, String name, StramElement childType) {
    List<T> childConfs = new ArrayList<T>();
    for (Conf conf : confs) {
      List<T> matchingConfs = conf.getMatchingChildConf(name, childType);
      childConfs.addAll(matchingConfs);
    }
    return childConfs;
  }

  private final Properties properties = new Properties();

  private StramConf stramConf = new StramConf();

  public LogicalPlanConfiguration() {
  }

  /**
   * Add operators from flattened name value pairs in configuration object.
   * @param conf
   */
  public void addFromConfiguration(Configuration conf) {
    addFromProperties(toProperties(conf, "stram."));
  }

  public static Properties toProperties(Configuration conf, String prefix) {
    Iterator<Entry<String, String>> it = conf.iterator();
    Properties props = new Properties();
    while (it.hasNext()) {
      Entry<String, String> e = it.next();
      // filter relevant entries
      if (e.getKey().startsWith(prefix)) {
         props.put(e.getKey(), e.getValue());
      }
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
  public String getAppAlias(String appPath) {
    String appAlias = null;
    if (appPath.endsWith(CLASS_SUFFIX)) {
      String className = appPath.replace("/", ".").substring(0, appPath.length()-CLASS_SUFFIX.length());
      appAlias = stramConf.appAliases.get(className);
    } else {
      appAlias = stramConf.appAliases.get(appPath);
    }
    return appAlias;
  }

  /**
   * Read node configurations from opProps. The opProps can be in any
   * random order, as long as they represent a consistent configuration in their
   * entirety.
   *
   * @param props
   * @return Logical plan configuration.
   */
  public LogicalPlanConfiguration addFromProperties(Properties props) {

    for (final String propertyName : props.stringPropertyNames()) {
      String propertyValue = props.getProperty(propertyName);
      this.properties.setProperty(propertyName, propertyValue);
      if (propertyName.startsWith(STRAM_PREFIX)) {
        String[] keyComps = propertyName.split("\\.");
        parseStramPropertyTokens(keyComps, 1, propertyName, propertyValue, stramConf);
      }
    }
    return this;
  }

  private void parseStramPropertyTokens(String[] keys, int index, String propertyName, String propertyValue, Conf conf) {
    if (index < keys.length) {
      String key = keys[index];
      StramElement element = getElement(key, conf);
      if ((element == StramElement.APPLICATION) || (element == StramElement.OPERATOR) || (element == StramElement.STREAM)
              || (element == StramElement.PORT) || (element == StramElement.INPUT_PORT) || (element == StramElement.OUTPUT_PORT)
              || ((element == StramElement.TEMPLATE))) {
        if ((index + 1) < keys.length) {
          String name = keys[index+1];
          Conf elConf = addConf(element, name, conf);
          parseStramPropertyTokens(keys, index+2, propertyName, propertyValue, elConf);
        } else {
          LOG.warn("Invalid configuration key: {}", propertyName);
          return;
        }
      } else if (element == StramElement.GATEWAY) {
        Conf elConf = addConf(element, null, conf);
        parseStramPropertyTokens(keys, index+1, propertyName, propertyValue, elConf);
      } else if ((element == StramElement.ATTR) || ((element == null) && (conf.getElement() == null))
                      || ((element == null) && (conf.getElement() == StramElement.GATEWAY))) {
        // Supporting current implementation where attribute can be directly specified under stram
        String attr = null;
        // Re-composing complete key for nested keys which are used in templates
        // Implement it better way to not pre-tokenize the property string and parse progressively
        if (element == StramElement.ATTR) {
          attr = getCompleteKey(keys, index + 1);
        } else {
          attr = getCompleteKey(keys, index);
          LOG.warn("Please specify the attribute {} using the {} keyword as {}", attr, StramElement.ATTR.getValue(),
                          getCompleteKey(keys, 0, index) + "." + StramElement.ATTR.getValue() +  "." + getCompleteKey(keys, index));
        }
        if (conf.getElement() == null) {
          conf = addConf(StramElement.APPLICATION, WILDCARD, conf);
        }
        if (attr != null) {
          conf.setAttribute(attr, propertyValue);
        } else {
          LOG.warn("Invalid attribute specification, no attribute name specified for {}", propertyName);
          return;
        }
      } else if (((element == StramElement.PROP) || (element == null))
              && ((conf.getElement() == StramElement.OPERATOR) || (conf.getElement() == StramElement.STREAM) || (conf.getElement() == StramElement.TEMPLATE))) {
        // Currently opProps are only supported on operators and streams
        // Supporting current implementation where property can be directly specified under operator
        String prop = null;
        if (element == StramElement.PROP) {
          prop = getCompleteKey(keys, index+1);
        } else {
          prop = getCompleteKey(keys, index);
          LOG.warn("Please specify the property {} using the {} keyword as {}", prop, StramElement.PROP.getValue(),
                        getCompleteKey(keys, 0, index) + "." + StramElement.PROP.getValue() + "." + getCompleteKey(keys, index));
        }
        if (prop != null) {
          conf.setProperty(prop, propertyValue);
        } else {
          LOG.warn("Invalid property specification, no property name specified for {}", propertyName);
          return;
        }
      } else if (element != null) {
        conf.parseElement(element, keys, index, propertyValue);
      }
    }
  }

  private StramElement getElement(String value, Conf conf) {
    StramElement element = null;
    try {
      element = StramElement.fromValue(value);
    } catch (IllegalArgumentException ie) {
    }
    // If element is not allowed treat it as text
    if ((element != null) && !conf.isAllowedElement(element)) {
      element = null;
    }
    return element;
  }

  private String getCompleteKey(String[] keys, int start) {
    return getCompleteKey(keys, start, keys.length);
  }

  private String getCompleteKey(String[] keys, int start, int end) {
    StringBuilder sb = new StringBuilder();
    for (int i = start; i < end; ++i) {
      if (i > start) sb.append(".");
      sb.append(keys[i]);
    }
    return sb.toString();
  }

  /**
   * Return all opProps set on the builder.
   * Can be serialized to property file and used to read back into builder.
   * @return Properties
   */
  public Properties getProperties() {
    return this.properties;
  }

  public Map<String, String> getAppAliases() {
    return Collections.unmodifiableMap(this.stramConf.appAliases);
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf) {

    Configuration pconf = new Configuration(conf);
    for (final String propertyName : this.properties.stringPropertyNames()) {
      String propertyValue = this.properties.getProperty(propertyName);
      pconf.setIfUnset(propertyName, propertyValue);
    }

    AppConf appConf = this.stramConf.getChild(WILDCARD, StramElement.APPLICATION);
    if (appConf == null) {
      throw new IllegalArgumentException(String.format("Application configuration not found"));
    }

    Map<String, OperatorConf> operators = appConf.getChildren(StramElement.OPERATOR);

    Map<OperatorConf, Operator> nodeMap = new HashMap<OperatorConf, Operator>(operators.size());
    // add all operators first
    for (Map.Entry<String, OperatorConf> nodeConfEntry : operators.entrySet()) {
      OperatorConf nodeConf = nodeConfEntry.getValue();
      Class<? extends Operator> nodeClass = StramUtils.classForName(nodeConf.getClassNameReqd(), Operator.class);
      Operator nd = dag.addOperator(nodeConfEntry.getKey(), nodeClass);
      setOperatorProperties(nd, nodeConf.getProperties());
      nodeMap.put(nodeConf, nd);
    }

    Map<String, StreamConf> streams = appConf.getChildren(StramElement.STREAM);

    // wire operators
    for (Map.Entry<String, StreamConf> streamConfEntry : streams.entrySet()) {
      StreamConf streamConf = streamConfEntry.getValue();
      DAG.StreamMeta sd = dag.addStream(streamConfEntry.getKey());
      sd.setLocality(streamConf.getLocality());

      if (streamConf.sourceNode != null) {
        String portName = null;
        for (Map.Entry<String, StreamConf> e : streamConf.sourceNode.outputs.entrySet()) {
          if (e.getValue() == streamConf) {
            portName = e.getKey();
          }
        }
        Operator sourceDecl = nodeMap.get(streamConf.sourceNode);
        Operators.PortMappingDescriptor sourcePortMap = new Operators.PortMappingDescriptor();
        Operators.describe(sourceDecl, sourcePortMap);
        sd.setSource(sourcePortMap.outputPorts.get(portName).component);
      }

      for (OperatorConf targetNode : streamConf.targetNodes) {
        String portName = null;
        for (Map.Entry<String, StreamConf> e : targetNode.inputs.entrySet()) {
          if (e.getValue() == streamConf) {
            portName = e.getKey();
          }
        }
        Operator targetDecl = nodeMap.get(targetNode);
        Operators.PortMappingDescriptor targetPortMap = new Operators.PortMappingDescriptor();
        Operators.describe(targetDecl, targetPortMap);
        sd.addSink(targetPortMap.inputPorts.get(portName).component);
      }
    }

  }

  /**
   * Populate the logical plan from the streaming application definition and configuration.
   * Configuration is resolved based on application alias, if any.
   * @param app
   * @param dag
   * @param name
   * @param conf
   */
  public void prepareDAG(LogicalPlan dag, StreamingApplication app, String name, Configuration conf) {
    String appAlias = getAppAlias(name);

    List<AppConf> appConfs = stramConf.getMatchingChildConf(appAlias, StramElement.APPLICATION);

    // set application level attributes first to make them available to populateDAG
    setApplicationConfiguration(dag, appConfs);

    app.populateDAG(dag, conf);

    if (appAlias != null) {
      dag.setAttribute(DAG.APPLICATION_NAME, appAlias);
    } else {
      if (dag.getAttributes().get(DAG.APPLICATION_NAME) == null) {
        dag.getAttributes().put(DAG.APPLICATION_NAME, name);
      }
    }
    // inject external operator configuration
    setOperatorConfiguration(dag, appConfs, appAlias);
    setStreamConfiguration(dag, appConfs, appAlias);
  }

  public static StreamingApplication create(Configuration conf, String tplgPropsFile) throws IOException {
    Properties topologyProperties = readProperties(tplgPropsFile);
    LogicalPlanConfiguration tb = new LogicalPlanConfiguration();
    tb.addFromProperties(topologyProperties);
    return tb;
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
  public Map<String, String> getProperties(OperatorMeta ow, String appName) {
    List<AppConf> appConfs = stramConf.getMatchingChildConf(appName, StramElement.APPLICATION);
    List<OperatorConf> opConfs = getMatchingChildConf(appConfs, ow.getName(), StramElement.OPERATOR);
    return getProperties(ow, opConfs, appName);
  }

  /**
   * Get the configuration opProps for the given operator.
   * These can be operator specific settings or settings from matching templates.
   * @param ow
   * @param opConfs
   * @param appName
   */
  private Map<String, String> getProperties(OperatorMeta ow, List<OperatorConf> opConfs, String appName) {

    List<TemplateConf> templateConfs = new ArrayList<TemplateConf>();

    // if there are properties set directly, an entry exists
    // else it will be created so we can evaluate the templates against it
    //OperatorConf n = getOrAddOperator(ow.getName());
    //n.opProps.put(OPERATOR_CLASSNAME, ow.getOperator().getClass().getName());

    Map<String, String> opProps = new HashMap<String, String>();
    Map<String, TemplateConf> templates = stramConf.getChildren(StramElement.TEMPLATE);
    // list of all templates that match operator, ordered by priority
    if (!templates.isEmpty()) {
      TreeMap<Integer, TemplateConf> matchingTemplates = getMatchingTemplates(ow, appName, templates);
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
    for (Conf conf : opConfs) {
      opProps.putAll(Maps.fromProperties(conf.properties));
    }
    //properties.remove(OPERATOR_CLASSNAME);
    return opProps;
  }

  private List<TemplateConf> getDirectTemplates(List<OperatorConf> opConfs, Map<String, TemplateConf> templates) {
    List<TemplateConf> refTemplates = new ArrayList<TemplateConf>();
    for (TemplateConf t : templates.values()) {
      for (OperatorConf opConf : opConfs) {
        if (t.id == opConf.templateRef) {
          refTemplates.add(t);
        }
      }
    }
    return refTemplates;
  }

  /**
   * Produce the collections of templates that apply for the given id.
   * @param ow
   * @param appName
   * @param templates
   * @return TreeMap<Integer, TemplateConf>
   */
  private TreeMap<Integer, TemplateConf> getMatchingTemplates(OperatorMeta ow, String appName, Map<String, TemplateConf> templates) {
    TreeMap<Integer, TemplateConf> tm = new TreeMap<Integer, TemplateConf>();
    for (TemplateConf t : templates.values()) {
      /*if (t.id == nodeConf.templateRef) {
        // directly assigned applies last
        tm.put(1, t);
        continue;
      } else*/ if ((t.idRegExp != null && ow.getName().matches(t.idRegExp))) {
        tm.put(1, t);
        continue;
      } else if (appName != null && t.appNameRegExp != null
          && appName.matches(t.appNameRegExp)) {
        tm.put(2, t);
        continue;
      } else if (t.classNameRegExp != null
          && ow.getOperator().getClass().getName().matches(t.classNameRegExp)) {
        tm.put(3, t);
        continue;
      }
    }
    return tm;
  }

  /**
   * Inject the configuration opProps into the operator instance.
   * @param operator
   * @param opProps
   * @return Operator
   */
  public static Operator setOperatorProperties(Operator operator, Map<String, String> properties)
  {
    try {
      // populate custom opProps
      BeanUtils.populate(operator, properties);
      return operator;
    }
    catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Error setting operator properties", e);
    }
    catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Error setting operator properties", e);
    }
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> getOperatorProperties(Operator operator)
  {
    return new BeanMap(operator);
  }

  private void setOperatorProperties(OperatorMeta ow, List<OperatorConf> opConfs, String appName) {
      Map<String, String> opProps = getProperties(ow, opConfs, appName);
      setOperatorProperties(ow.getOperator(), opProps);
  }

  /**
   * Set any opProps from configuration on the operators in the DAG. This
   * method may throw unchecked exception if the configuration contains
   * opProps that are invalid for an operator.
   *
   * @param dag
   * @param applicationName
   */
  public void setOperatorProperties(LogicalPlan dag, String applicationName) {
    List<AppConf> appConfs = stramConf.getMatchingChildConf(applicationName, StramElement.APPLICATION);
    for (OperatorMeta ow : dag.getAllOperators()) {
      List<OperatorConf> opConfs = getMatchingChildConf(appConfs, ow.getName(), StramElement.OPERATOR);
      Map<String, String> opProps = getProperties(ow, opConfs, applicationName);
      setOperatorProperties(ow.getOperator(), opProps);
    }
  }

  private static final Map<Attribute<?>, String> legacyKeyMap = Maps.newHashMap();

  static {
    legacyKeyMap.put(DAGContext.APPLICATION_NAME, "appName");
    legacyKeyMap.put(DAGContext.LIBRARY_JARS, "libjars");
    legacyKeyMap.put(DAGContext.CONTAINERS_MAX_COUNT, "maxContainers");
    legacyKeyMap.put(DAGContext.CONTAINER_MEMORY_MB, "containerMemoryMB");
    legacyKeyMap.put(DAGContext.CONTAINER_JVM_OPTIONS, "containerJvmOpts");
    legacyKeyMap.put(DAGContext.MASTER_MEMORY_MB, "masterMemoryMB");
    legacyKeyMap.put(DAGContext.STREAMING_WINDOW_SIZE_MILLIS, "windowSizeMillis");
    legacyKeyMap.put(DAGContext.APPLICATION_PATH, "appPath");
    legacyKeyMap.put(DAGContext.RESOURCE_ALLOCATION_TIMEOUT_MILLIS, "allocateResourceTimeoutMillis");
  }

  /**
   * Set the application configuration.
   * @param dag
   * @param appName
   */
  public void setApplicationConfiguration(final LogicalPlan dag, String appName) {
    List<AppConf> appConfs = stramConf.getMatchingChildConf(appName, StramElement.APPLICATION);
    setApplicationConfiguration(dag, appConfs);
  }

  private void setApplicationConfiguration(final LogicalPlan dag, List<AppConf> appConfs) {

    for (Conf appConf : appConfs) {
      Conf gwConf = appConf.getChild(null, StramElement.GATEWAY);
      if (gwConf != null) {
        String gatewayAddress = gwConf.attributes.getProperty(GATEWAY_ADDRESS_ATTR);
        if (gatewayAddress !=  null) {
          dag.setAttribute(DAGContext.GATEWAY_ADDRESS, gatewayAddress);
          break;
        }
      }
    }

    LOG.debug("Initializing DAGContext!", DAGContext.serialVersionUID); /* make sure that the DAGContext.class is initialized */
    setAttributes(DAGContext.class, appConfs, new AttributeAdapter() {

      @Override
      public void setAttribute(Attribute<Object> attribute, Object value)
      {
        dag.setAttribute(attribute, value);
      }

    });
  }

  private void setOperatorConfiguration(final LogicalPlan dag, List<AppConf> appConfs, String appName) {
    for (final OperatorMeta ow : dag.getAllOperators()) {
      List<OperatorConf> opConfs = getMatchingChildConf(appConfs, ow.getName(), StramElement.OPERATOR);
      // Set the operator attributes
      setAttributes(OperatorContext.class, opConfs, new AttributeAdapter() {

        @Override
        public void setAttribute(Attribute<Object> attribute, Object value)
        {
          dag.setAttribute(ow.getOperator(), attribute, value);
        }

      });

      // Set the operator opProps
      Map<String, String> opProps = getProperties(ow, opConfs, appName);
      setOperatorProperties(ow.getOperator(), opProps);

      // Set the port attributes
      for (Entry<LogicalPlan.InputPortMeta, LogicalPlan.StreamMeta> entry : ow.getInputStreams().entrySet()) {
        final InputPortMeta im = entry.getKey();
        List<PortConf> inPortConfs = getMatchingChildConf(opConfs, im.getPortName(), StramElement.INPUT_PORT);
        // Add the generic port attributes as well
        List<PortConf> portConfs = getMatchingChildConf(opConfs, im.getPortName(), StramElement.PORT);
        inPortConfs.addAll(portConfs);
        setAttributes(PortContext.class, inPortConfs, new AttributeAdapter() {

          @Override
          public void setAttribute(Attribute<Object> attribute, Object value)
          {
            dag.setInputPortAttribute(im.getPortObject(), attribute, value);
          }

        });
      }

      for (Entry<LogicalPlan.OutputPortMeta, LogicalPlan.StreamMeta> entry : ow.getOutputStreams().entrySet()) {
        final OutputPortMeta om = entry.getKey();
        List<PortConf> outPortConfs = getMatchingChildConf(opConfs, om.getPortName(), StramElement.OUTPUT_PORT);
        // Add the generic port attributes as well
        List<PortConf> portConfs = getMatchingChildConf(opConfs, om.getPortName(), StramElement.PORT);
        outPortConfs.addAll(portConfs);
        setAttributes(PortContext.class, outPortConfs, new AttributeAdapter() {

          @Override
          public void setAttribute(Attribute<Object> attribute, Object value)
          {
            dag.setOutputPortAttribute(om.getPortObject(), attribute, value);
          }

        });
      }
    }
  }

  private void setStreamConfiguration(LogicalPlan dag, List<AppConf> appConfs, String appAlias) {
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

  private void setAttributes(Class<?> clazz, List<? extends Conf> confs, AttributeAdapter adapter) {
    Set<Attribute<Object>> attributes = AttributeInitializer.getAttributes(clazz);
    if (confs.size() > 0) {
      for (Attribute<Object> attribute : attributes) {
        String simpleName = attribute.name.substring(attribute.name.lastIndexOf('.')+1);
        for (Conf conf : confs) {
          String stringValue = conf.attributes.getProperty(simpleName);
          if (stringValue == null) {
            String legacyKey = legacyKeyMap.get(attribute);
            if (legacyKey != null) {
              stringValue = conf.attributes.getProperty(legacyKey);
            } else {
              /* This is temporary fix till we get rid of old inconsistent names from our code. */
              legacyKey = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, simpleName);
              stringValue = conf.attributes.getProperty(legacyKey);
            }
            if (stringValue != null) {
              LOG.warn("Referencing the attribute as {} instead of {} is deprecated!", legacyKey, simpleName);
            }
          }
          if (stringValue != null) {
            if (attribute.codec == null) {
              String msg = String.format("Unsupported attribute type: %s (%s)", attribute.codec, attribute.name);
              throw new UnsupportedOperationException(msg);
            }
            else {
              adapter.setAttribute(attribute, attribute.codec.fromString(stringValue));
            }
            break;
          }
        }
      }
    }
  }

  private interface AttributeAdapter  {
    public void setAttribute(Attribute<Object> attribute, Object value);
  }

}
