package com.datatorrent.stram.util;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.hadoop.conf.Configuration;

public class ExternalizableConf implements Externalizable
{
  private final Configuration conf;

  public ExternalizableConf(Configuration conf)
  {
    this.conf = conf;
  }

  public ExternalizableConf()
  {
    this.conf = new Configuration(false);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
  {
    conf.readFields(in);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException
  {
    conf.write(out);
  }
}