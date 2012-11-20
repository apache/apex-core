/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.storage;

import com.google.common.io.Files;
import java.io.*;
import java.util.Arrays;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class DiskStorage implements Storage
{
  final String basePath;

  public DiskStorage(String baseDirectory)
  {
    basePath = baseDirectory;
  }

  public DiskStorage() throws IOException
  {
    basePath = File.createTempFile("tt", "tt").getParent();
  }

  public Storage getInstance() throws IOException
  {
    return new DiskStorage();
  }

  public static String normalizeFileName(String name)
  {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < name.length(); i++) {
      Character c = name.charAt(i);
      if (Character.isLetterOrDigit(c)) {
        sb.append(c);
      }
      else {
        sb.append('-');
      }
    }

    return sb.toString();
  }

  private File getNextFileName(String identifier, String filename)
  {
    String normalizedFileName = normalizeFileName(identifier);
    File directory = new File(basePath, normalizedFileName);
    if (directory.isDirectory()) {
      /* make sure that it's the directory for the current identifier */
      File identityFile = new File(directory, "identity");
      if (identityFile.isFile()) {
        try {
          byte[] stored = Files.toByteArray(identityFile);
          if (Arrays.equals(stored, identifier.getBytes())) {
            String[] sfiles = directory.list();
            Arrays.sort(sfiles);

            for (String s: sfiles) {
              if (!s.equals("identity") && s.compareTo(filename) > 0) {
                return new File(directory, s);
              }
            }
          }
        }
        catch (IOException ex) {
        }
      }
    }

    return null;
  }

  public Block retrieveFirstBlock(final String identifier)
  {
    final File file = getNextFileName(identifier, "");
    return retrieveBlock(file, identifier);
  }

  public Block retrieveNextBlock(Block block)
  {
    final File file = getNextFileName(block.getIdentifier(), block.getNumber());
    return retrieveBlock(file, block.getIdentifier());
  }

  public Block storeFirstBlock(final String identifier, byte[] bytes, int startingOffset, int endingOffset)
  {
    String normalizedFileName = normalizeFileName(identifier);
    File directory = new File(basePath, normalizedFileName);
    if (directory.exists()) {
      // clean it up!
      throw new RuntimeException("directory " + directory.getAbsolutePath() + " exists!");
    }
    else {
      if (directory.mkdir()) {
        File identity = new File(directory, "identity");
        try {
          Files.write(identifier.getBytes(), identity);
        }
        catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
      else {
        throw new RuntimeException("directory " + directory.getAbsolutePath() + " could not be created!");
      }
    }

    try {
      final byte[] newbytes;
      if (startingOffset > 0 || endingOffset < bytes.length) {
        newbytes = new byte[endingOffset - startingOffset];
        System.arraycopy(bytes, startingOffset, newbytes, 0, endingOffset - startingOffset);
      }
      else {
        newbytes = bytes;
      }
      Files.write(newbytes, new File(directory, "1"));

      return new Block()
      {
        public String getIdentifier()
        {
          return identifier;
        }

        public String getNumber()
        {
          return "1";
        }

        public byte[] getBytes()
        {
          return newbytes;
        }
      };
    }
    catch (IOException ex) {
    }

    return null;
  }

  public Block delete(Block block)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public void write(File file, byte[] bytes) throws IOException
  {
  }

  public byte[] read(File file) throws IOException
  {
    ByteArrayOutputStream ous = new ByteArrayOutputStream();
    InputStream ios = new FileInputStream(file);
    try {
      byte[] buffer = new byte[4096];
      int read;
      while ((read = ios.read(buffer)) != -1) {
        ous.write(buffer, 0, read);
      }
    }
    finally {
      try {
        if (ous != null) {
          ous.close();
        }
      }
      catch (IOException e) {
      }

      try {
        if (ios != null) {
          ios.close();
        }
      }
      catch (IOException e) {
      }
    }
    return ous.toByteArray();
  }

  protected Block retrieveBlock(final File file, final String identifier)
  {
    try {
      final byte[] contents = read(file);
      return new Block()
      {
        public String getIdentifier()
        {
          return identifier;
        }

        public String getNumber()
        {
          return file.getName();
        }

        public byte[] getBytes()
        {
          return contents;
        }
      };
    }
    catch (IOException ex) {
    }

    return null;
  }

  public Block storeNextBlock(Block block, byte[] bytes, int startingOffset, int endingOffset)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
