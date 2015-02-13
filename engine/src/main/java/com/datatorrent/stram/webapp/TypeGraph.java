package com.datatorrent.stram.webapp;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.InnerClassNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TypeGraph
{
  
  private static final Logger LOG = LoggerFactory.getLogger(TypeGraph.class);

  private final Map<String, TypeGraphVertex> typeGraph = new HashMap<String, TypeGraphVertex>();

  public void addNode(ClassReader reader)
  {
    ClassNode cn = new ClassNode();
    reader.accept(cn, ClassReader.SKIP_DEBUG);
    String typeName = cn.name.replace('/', '.');

    int opcode = reader.getAccess();
    @SuppressWarnings("unchecked")
    List<InnerClassNode> icl = cn.innerClasses;
    if (typeName.contains("$")) {
      for (InnerClassNode innerClassNode : icl) {
        if (innerClassNode.name.replace('/', '.').equals(typeName)) {
          opcode = innerClassNode.access;
        }
      }
    }

    TypeGraphVertex tgv = null;
    TypeGraphVertex ptgv = null;
    if (typeGraph.containsKey(typeName)) {
      tgv = typeGraph.get(typeName);
      tgv.setOpCode(opcode);
    } else {
      tgv = new TypeGraphVertex(typeName, opcode);
      typeGraph.put(typeName, tgv);
    }
    String immediateP = reader.getSuperName();
    if (immediateP != null) {
      immediateP = immediateP.replace('/', '.');
      ptgv = typeGraph.get(immediateP);
      if (ptgv == null) {
        ptgv = new TypeGraphVertex(immediateP);
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
          ptgv = new TypeGraphVertex(iface);
          typeGraph.put(iface, ptgv);
        }
        tgv.ancestors.add(ptgv);
        ptgv.descendants.add(tgv);
      }
    }

    updatePublicConcreteDescendants(tgv);

  }

  private void updatePublicConcreteDescendants(TypeGraphVertex tgv)
  {
    for (TypeGraphVertex parent : tgv.ancestors) {
      updatePublicConcreteDescendants(parent, tgv.allPublicConcreteDescendants, tgv.isPublicConcrete() ? tgv : null);
    }
  }

  private void updatePublicConcreteDescendants(TypeGraphVertex tgv, Set<TypeGraphVertex> indirectChildren, TypeGraphVertex newNode)
  {

    tgv.allPublicConcreteDescendants.addAll(indirectChildren);
    if (newNode != null) {
      tgv.allPublicConcreteDescendants.add(newNode);
    }

    for (TypeGraphVertex parent : tgv.ancestors) {
      updatePublicConcreteDescendants(parent, indirectChildren, newNode);
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

  public Set<String> getPublicConcreteDescendants(String fullClassName, int limit)
  {

    Set<String> result = new HashSet<String>();
    TypeGraphVertex tgv = typeGraph.get(fullClassName);

    if (tgv.numberOfPublicConcreteDescendants() > limit) {
      throw new RuntimeException("Too many public concrete sub types!");
    }
    if (tgv != null) {
      for (TypeGraphVertex node : tgv.allPublicConcreteDescendants) {
        result.add(node.typeName);
      }
    }
    return result;
  }

  private void tranverse(TypeGraphVertex tgv, boolean onlyPublicConcrete, Set<String> result, int limit)
  {
    if (!onlyPublicConcrete) {
      result.add(tgv.typeName);
    }

    if (onlyPublicConcrete && tgv.numberOfPublicConcreteDescendants() > limit) {
      throw new RuntimeException("Too many public concrete sub types!");
    }

    if (onlyPublicConcrete && tgv.isPublicConcrete()) {
      result.add(tgv.typeName);
    }

    if (tgv.descendants.size() > 0) {
      for (TypeGraphVertex child : tgv.descendants) {
        tranverse(child, onlyPublicConcrete, result, limit);
      }
    }
  }

  public static class TypeGraphVertex
  {

    /**
     * Vertex is unique by name, hashCode and equal depends only on typeName
     */
    public final String typeName;

    private int opCode = -1;

    public Class<? extends Object> loadedClass = null;

    private final Set<TypeGraphVertex> allPublicConcreteDescendants = new HashSet<TypeGraphVertex>();

    private final Set<TypeGraphVertex> ancestors = new HashSet<TypeGraphVertex>();

    private final Set<TypeGraphVertex> descendants = new HashSet<TypeGraphVertex>();

    public TypeGraphVertex(String typeName, int opCode)
    {

      this.typeName = typeName;
      this.opCode = opCode;
    }

    public int numberOfPublicConcreteDescendants()
    {
      return allPublicConcreteDescendants.size() + (isPublicConcrete() ? 1 : 0);
    }

    public TypeGraphVertex(String typeName)
    {
      this.typeName = typeName;
    }

    public void setOpCode(int opCode)
    {
      this.opCode = opCode;
    }

    private boolean isPublicConcrete()
    {
      if (opCode < 0) {
        // If the class is not in the classpath
        return false;
      }
      // if the class is neither abstract nor interface
      // and the class is public
      return ((opCode & (Opcodes.ACC_ABSTRACT | Opcodes.ACC_INTERFACE)) == 0) && ((opCode & Opcodes.ACC_PUBLIC) == Opcodes.ACC_PUBLIC);
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
  }

  public void loadAllSubClasses(Class<? extends Object> clazz)
  {
    TypeGraphVertex tgv = typeGraph.get(clazz.getName());
    if(tgv == null) {
      return;
    }
    tgv.loadedClass = clazz;
    for (TypeGraphVertex subType : tgv.allPublicConcreteDescendants) {
      try {
        subType.loadedClass = Class.forName(subType.typeName);
      } catch (ClassNotFoundException e) {
        LOG.warn("Load class {} error", clazz, e);
      }
    }
  }
  
  public void loadAllSubClasses(String clazzName)
  {
    TypeGraphVertex tgv = typeGraph.get(clazzName);
    if(tgv == null) {
      return;
    } else
      try {
        loadAllSubClasses(Class.forName(clazzName));
      } catch (ClassNotFoundException e) {
        LOG.warn("Load class {} error", clazzName, e);
      }
  }

  public void loadClass(Class<? extends Object> clazz)
  {
    TypeGraphVertex tgv = typeGraph.get(clazz.getName());
    if(tgv == null) {
      return;
    }
    tgv.loadedClass = clazz;
  }

}
