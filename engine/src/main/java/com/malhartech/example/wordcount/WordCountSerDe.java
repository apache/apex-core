/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.example.wordcount;

import com.malhartech.dag.DefaultSerDe;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class WordCountSerDe extends DefaultSerDe
{

  final byte[][] partition = new byte[][]{
    {'a'}, {'b'}, {'c'}, {'d'}, {'e'},
    {'f'}, {'g'}, {'h'}, {'i'}, {'j'},
    {'k'}, {'l'}, {'m'}, {'n'}, {'o'},
    {'p'}, {'q'}, {'r'}, {'s'}, {'t'},
    {'u'}, {'v'}, {'w'}, {'x'}, {'y'},
    {'z'}
  };
  
  final byte[] defaultPartition = new byte[]{'#'};

  @Override
  public byte[] getPartition(Object o)
  {
    char c = ((WordHolder) o).word.charAt(0);
    if (Character.isLetter(c)) {
      return partition[(Character.toLowerCase(c) - 'a') % 26];
    }
    
    return defaultPartition;
  }
  
  @Override
  public byte[] toByteArray(Object o)
  {
    if (o instanceof String) {
      return ((String)o).getBytes();
    }
    
    return super.toByteArray(o);
  }
}
