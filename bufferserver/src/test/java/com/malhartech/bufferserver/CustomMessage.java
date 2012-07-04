/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.GeneratedMessage.Builder;
import com.malhartech.bufferserver.Buffer.BeginWindow;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author chetan
 */
public class CustomMessage
{

  static class SomeClass
  {
    final static Descriptors.Descriptor descriptor = Buffer.BeginWindow.getDescriptor();
    final static BeginWindow.Builder builder = BeginWindow.newBuilder();
    
    public Descriptors.Descriptor getMessageDescriptor()
    {
      return descriptor;
    }

    public byte[] getObject()
    {
      Buffer.BeginWindow.Builder builder = Buffer.BeginWindow.newBuilder();
      builder.setNode("chetan narsude");
      
      return builder.build().toByteArray();
    }
    
    public Builder getBuilder()
    {
      return builder;
    }
  }
  
  public static final String filename = "abc.bin";

  public static void main(String argv[]) throws IOException
  {
    SomeClass sc = new SomeClass();
    if (argv.length > 0) {
      try {
        FileOutputStream fstream = new FileOutputStream(filename);
        fstream.write(sc.getObject());
        fstream.close();
      }
      catch (FileNotFoundException ex) {
        Logger.getLogger(CustomMessage.class.getName()).log(Level.SEVERE, null, ex);
      }
    }
    else {
      FileInputStream fstream  = new FileInputStream(filename);
      byte bytes[] = new byte[fstream.available()];
      System.out.println("read " + fstream.read(bytes) + " bytes");
      DynamicMessage dm = DynamicMessage.parseFrom(sc.getMessageDescriptor(), bytes);
      BeginWindow bw = (BeginWindow) sc.getBuilder().mergeFrom(dm).build();
      System.out.println("name = " + bw.getNode());
    }
  }
}
