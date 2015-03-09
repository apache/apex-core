package com.datatorrent.stram.webapp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.text.WordUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.signature.SignatureReader;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.InnerClassNode;
import org.objectweb.asm.tree.MethodNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.stram.webapp.asm.ASMUtil;
import com.datatorrent.stram.webapp.asm.MethodSignatureVisitor;
import com.datatorrent.stram.webapp.asm.Type;
import com.datatorrent.stram.webapp.asm.Type.ParameterizedTypeNode;
import com.datatorrent.stram.webapp.asm.Type.TypeNode;
import com.datatorrent.stram.webapp.asm.Type.WildcardTypeNode;

public class TypeGraph
{

  enum UI_TYPE {

    LIST(Collection.class.getName(), "List"),

    ENUM(Enum.class.getName(), "Enum"),

    MAP(Map.class.getName(), "Map");

    private final String assignableTo;
    private final String name;

    private UI_TYPE(String assignableTo, String name)
    {
      this.assignableTo = assignableTo;
      this.name = name;
    }

    public static UI_TYPE getEnumFor(String clazzName, Map<String, TypeGraphVertex> typeGraph)
    {
      TypeGraphVertex tgv = typeGraph.get(clazzName);
      if (tgv == null) {
        return null;
      }
      for (UI_TYPE type : UI_TYPE.values()) {
        TypeGraphVertex typeTgv = typeGraph.get(type.assignableTo);
        if (typeTgv == null) {
          continue;
        }
        if (isAncestor(typeTgv, tgv)) {
          return type;
        }
      }
      return null;
    }

    private static boolean isAncestor(TypeGraphVertex typeTgv, TypeGraphVertex tgv)
    {
      if (tgv == typeTgv) {
        return true;
      }
      if ((tgv.ancestors == null || tgv.ancestors.size() == 0)) {
        return false;
      }
      for (TypeGraphVertex vertex : tgv.ancestors) {
        if (isAncestor(typeTgv, vertex))
          return true;
      }
      return false;
    }

    public String getName()
    {
      return name;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(TypeGraph.class);

  private final Map<String, TypeGraphVertex> typeGraph = new HashMap<String, TypeGraphVertex>();

  private void addNode(InputStream input, String resName) throws IOException
  {
    try {

      ClassReader reader = new ClassReader(input);
      ClassNode cn = new ClassNode();
      reader.accept(cn, ClassReader.SKIP_CODE);
      String typeName = cn.name.replace('/', '.');

      TypeGraphVertex tgv = null;
      TypeGraphVertex ptgv = null;
      if (typeGraph.containsKey(typeName)) {
        tgv = typeGraph.get(typeName);
        tgv.setAsmNode(cn);
      } else {
        tgv = new TypeGraphVertex(typeName, resName, cn);
        typeGraph.put(typeName, tgv);
      }
      String immediateP = reader.getSuperName();
      if (immediateP != null) {
        immediateP = immediateP.replace('/', '.');
        ptgv = typeGraph.get(immediateP);
        if (ptgv == null) {
          ptgv = new TypeGraphVertex(immediateP, resName);
          typeGraph.put(immediateP, ptgv);
        }
        tgv.ancestors.add(ptgv);
        ptgv.descendants.add(tgv);
      }
      if (reader.getInterfaces() != null) {
        for (String iface : reader.getInterfaces()) {
          iface = iface.replace('/', '.');
          ptgv = typeGraph.get(iface);
          if (ptgv == null) {
            ptgv = new TypeGraphVertex(iface, resName);
            typeGraph.put(iface, ptgv);
          }
          tgv.ancestors.add(ptgv);
          ptgv.descendants.add(tgv);
        }
      }

      updateInitializableDescendants(tgv);
    } finally {
      if (input != null) {
        input.close();
      }
    }
  }

  public void addNode(File file) throws IOException
  {
    addNode(new FileInputStream(file), file.getAbsolutePath());
  }

  public void addNode(JarEntry jarEntry, JarFile jar) throws IOException
  {
    addNode(jar.getInputStream(jarEntry), jar.getName());
  }

  private void updateInitializableDescendants(TypeGraphVertex tgv)
  {
    for (TypeGraphVertex parent : tgv.ancestors) {
      updateInitializableDescendants(parent, tgv.allInitialiazableDescendants, tgv.isInitializable() ? tgv : null);
    }
  }

  private void updateInitializableDescendants(TypeGraphVertex tgv, Set<TypeGraphVertex> indirectChildren, TypeGraphVertex newNode)
  {

    tgv.allInitialiazableDescendants.addAll(indirectChildren);
    if (newNode != null) {
      tgv.allInitialiazableDescendants.add(newNode);
    }

    for (TypeGraphVertex parent : tgv.ancestors) {
      updateInitializableDescendants(parent, indirectChildren, newNode);
    }
  }

  public int size()
  {
    return typeGraph.size();
  }

  public Set<String> getDescendants(String fullClassName)
  {
    Set<String> result = new HashSet<String>();
    TypeGraphVertex tgv = typeGraph.get(fullClassName);
    if (tgv != null) {
      tranverse(tgv, false, result, Integer.MAX_VALUE);
    }
    return result;
  }

  public Set<String> getInitializableDescendants(String fullClassName, int limit)
  {
    return getInitializableDescendants(fullClassName, limit, null, null);
  }

  private void tranverse(TypeGraphVertex tgv, boolean onlyInitializable, Set<String> result, int limit)
  {
    if (!onlyInitializable) {
      result.add(tgv.typeName);
    }

    if (onlyInitializable && tgv.numberOfInitializableDescendants() > limit) {
      throw new RuntimeException("Too many public concrete sub types!");
    }

    if (onlyInitializable && tgv.isInitializable()) {
      result.add(tgv.typeName);
    }

    if (tgv.descendants.size() > 0) {
      for (TypeGraphVertex child : tgv.descendants) {
        tranverse(child, onlyInitializable, result, limit);
      }
    }
  }

  public static class TypeGraphVertex
  {

    /**
     * Vertex is unique by name, hashCode and equal depends only on typeName
     */
    public final String typeName;

    private ClassNode asmNode = null;

    private final Set<TypeGraphVertex> allInitialiazableDescendants = new HashSet<TypeGraphVertex>();

    private final Set<TypeGraphVertex> ancestors = new HashSet<TypeGraphVertex>();

    private final Set<TypeGraphVertex> descendants = new HashSet<TypeGraphVertex>();

    // keep the jar file name for late fetching the detail information
    private final String jarName;

    public TypeGraphVertex(String typeName, String jarName, ClassNode asmNode)
    {

      this.jarName = typeName;
      this.typeName = typeName;
      this.asmNode = asmNode;
    }

    public int numberOfInitializableDescendants()
    {
      return allInitialiazableDescendants.size() + (isInitializable() ? 1 : 0);
    }

    public TypeGraphVertex(String typeName, String jarName)
    {
      this.typeName = typeName;
      this.jarName = jarName;
    }

    private boolean isInitializable()
    {
      return isPublicConcrete() && ASMUtil.getPublicDefaultConstructor(asmNode) != null;
    }

    private boolean isPublicConcrete()
    {
      if (asmNode == null) {
        // If the class is not in the classpath
        return false;
      }
      int opCode = getOpCode();

      // if the class is neither abstract nor interface
      // and the class is public
      return ((opCode & (Opcodes.ACC_ABSTRACT | Opcodes.ACC_INTERFACE)) == 0) && ((opCode & Opcodes.ACC_PUBLIC) == Opcodes.ACC_PUBLIC);
    }

    private int getOpCode()
    {
      @SuppressWarnings("unchecked")
      List<InnerClassNode> icl = asmNode.innerClasses;
      if (typeName.contains("$")) {
        for (InnerClassNode innerClassNode : icl) {
          if (innerClassNode.name.replace('/', '.').equals(typeName)) {
            return innerClassNode.access;
          }
        }
      }
      return asmNode.access;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((typeName == null) ? 0 : typeName.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      TypeGraphVertex other = (TypeGraphVertex) obj;
      if (typeName == null) {
        if (other.typeName != null)
          return false;
      } else if (!typeName.equals(other.typeName))
        return false;
      return true;
    }

    public String getJarName()
    {
      return jarName;
    }

    public ClassNode getAsmNode()
    {
      return asmNode;
    }

    public void setAsmNode(ClassNode asmNode)
    {
      this.asmNode = asmNode;
    }
  }

  public Set<String> getInitializableDescendants(String clazz, int limit, String filter, String packagePrefix)
  {
    Set<String> result = new HashSet<String>();
    TypeGraphVertex tgv = typeGraph.get(clazz);

    if (tgv.numberOfInitializableDescendants() > limit) {
      throw new RuntimeException("Too many public concrete sub types!");
    }
    if (tgv != null) {
      for (TypeGraphVertex node : tgv.allInitialiazableDescendants) {
        if (filter != null && !Pattern.matches(filter, node.typeName)) {
          continue;
        }
        if (packagePrefix != null && !node.typeName.startsWith(packagePrefix)) {
          continue;
        }
        result.add(node.typeName);
      }
    }
    return result;
  }

  public JSONObject describeClass(String clazzName) throws JSONException
  {
    JSONObject desc = new JSONObject();
    desc.put("name", clazzName);
    TypeGraphVertex tgv = typeGraph.get(clazzName);
    if (tgv == null) {
      return desc;
    }
    ClassNode cn = tgv.asmNode;
    if (ASMUtil.isEnum(cn)) {

      List<String> enumNames = ASMUtil.getEnumValues(cn);
      desc.put("enum", enumNames);
    }
    desc.put("properties", getClassProperties(clazzName));
    return desc;
  }

  private Collection<JSONObject> getClassProperties(String clazzName) throws JSONException
  {
    TypeGraphVertex tgv = typeGraph.get(clazzName);
    if (tgv == null) {
      return null;
    }
    Map<String, JSONObject> results = new HashMap<String, JSONObject>();
    List<MethodNode> getters = ASMUtil.getPublicGetter(tgv.getAsmNode());
    List<MethodNode> setters = ASMUtil.getPublicSetter(tgv.getAsmNode());

    for (MethodNode setter : setters) {
      String prop = WordUtils.uncapitalize(setter.name.substring(3));
      JSONObject propJ = results.get(prop);
      if (propJ == null) {
        propJ = new JSONObject();
        propJ.put("name", prop);
        results.put(prop, propJ);
      }
      propJ.put("canSet", "true");
      propJ.put("canGet", "false");

      // String typeString = null;
      // if(setter.signature == null){
      // Type t = Type.getArgumentTypes(setter.desc)[0];
      // if(t.getSort()==Type.ARRAY){
      // typeString = t.toString();
      // } else {
      // typeString = Type.getArgumentTypes(setter.desc)[0].getClassName();
      // }
      // }
      String sigString = setter.signature != null ? setter.signature : setter.desc;
      SignatureReader reader = new SignatureReader(sigString);
      MethodSignatureVisitor gss = new MethodSignatureVisitor();
      reader.accept(gss);
      List<Type> param = gss.getParameters();
      if (CollectionUtils.isEmpty(param)) {
        propJ.put("type", "UNKNOWN");
      } else {
        // only one param in setter method
        setTypes(propJ, param.get(0));
        // propJ.put("type", param.getTypeObj().getClassName());

      }
      // propJ.put("type", typeString);
    }

    for (MethodNode getter : getters) {
      int si = getter.name.startsWith("is") ? 2 : 3;
      String prop = WordUtils.uncapitalize(getter.name.substring(si));
      JSONObject propJ = results.get(prop);
      if (propJ == null) {
        propJ = new JSONObject();
        propJ.put("name", prop);
        results.put(prop, propJ);
        propJ.put("canSet", "false");
        // propJ.put("type", Type.getReturnType(getter.desc).getClassName());

        String sigString = getter.signature != null ? getter.signature : getter.desc;
        // System.out.println(sigString);
        SignatureReader reader = new SignatureReader(sigString);
        MethodSignatureVisitor gss = new MethodSignatureVisitor();
        reader.accept(gss);

        Type rt = gss.getReturnType();
        if (rt == null) {
          propJ.put("type", "UNKNOWN");
        } else {
          setTypes(propJ, rt);
          // propJ.put("type", param.getTypeObj().getClassName());

        }

      }

      propJ.put("canGet", "true");
    }

    return results.values();
  }

  private void setTypes(JSONObject propJ, Type t) throws JSONException
  {
    if (propJ == null) {
      return;
    } else {
      if (t instanceof WildcardTypeNode) {
        propJ.put("type", "?");
      } else if (t instanceof TypeNode) {
        TypeNode tn = (TypeNode) t;
        propJ.put("type", tn.getTypeObj().getClassName());
        UI_TYPE uiType = UI_TYPE.getEnumFor(tn.getTypeObj().getClassName(), typeGraph);
        if (uiType != null) {
          propJ.put("uiType", uiType.getName());
        }
        if (t instanceof ParameterizedTypeNode) {
          JSONArray jArray = new JSONArray();
          for (Type ttn : ((ParameterizedTypeNode) t).getActualTypeArguments()) {
            JSONObject objJ = new JSONObject();
            setTypes(objJ, ttn);
            jArray.put(objJ);
          }
          propJ.put("typeArgs", jArray);
        }
      }
      if (t instanceof WildcardTypeNode) {
        JSONObject typeBounds = new JSONObject();
        
        
        JSONArray jArray = new JSONArray();
        Type[] bounds = ((WildcardTypeNode) t).getUpperBounds();
        if(bounds!=null){
          for (Type type : bounds) {
            jArray.put(type.toString());
          }
        }
        typeBounds.put("upper", jArray);
        
        bounds = ((WildcardTypeNode) t).getLowerBounds();

        jArray = new JSONArray();
        if(bounds!=null){
          for (Type type : bounds) {
            jArray.put(type.toString());
          }
        }
        typeBounds.put("lower", jArray);

        propJ.put("typeBounds", typeBounds);

      }


    }
  }

}
