/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.storage;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
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

  @Override
  public int store(String identifier, byte[] bytes, int startingOffset, int endingOffset)
  {
    int filename = 0;

    String normalizedFileName = normalizeFileName(identifier);
    File directory = new File(basePath, normalizedFileName);
    if (directory.exists()) {
      File identityFile = new File(directory, "identity");
      if (identityFile.isFile()) {
        try {
          byte[] stored = Files.toByteArray(identityFile);
          if (Arrays.equals(stored, identifier.getBytes())) {
            String[] sfiles = directory.list();
            Arrays.sort(sfiles);
            for (int j = sfiles.length; j-- > 0;) {
              if (!sfiles[j].equals("identity")) {
                filename = Integer.parseInt(sfiles[j]) + 1;
              }
            }
          }
          else {
            throw new RuntimeException("Collission in identifier name, please ensure that the slug for the identifiers is differents");
          }
        }
        catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
      else {
        throw new RuntimeException("Identity file is hijacked!");
      }
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

    return writeFile(bytes, startingOffset, endingOffset, directory, filename == 0 ? 1 : filename);
  }

  public void discard(String identifier, int uniqueIdentifier)
  {
    String normalizedFilename = normalizeFileName(identifier);
    File directory = new File(basePath, normalizedFilename);
    if (directory.exists()) {
      File identityFile = new File(directory, "identity");
      if (identityFile.isFile()) {
        try {
          byte[] stored = Files.toByteArray(identityFile);
          if (Arrays.equals(stored, identifier.getBytes())) {
            File deletionFile = new File(directory, String.valueOf(uniqueIdentifier));
            if (deletionFile.exists() && deletionFile.isFile()) {
              if (!deletionFile.delete()) {
                throw new RuntimeException("File " + deletionFile.getPath() + " could not be deleted!");
              }
            }
            else {
              throw new RuntimeException("File " + deletionFile.getPath() + " either is non existent or not a file!");
            }
          }
          else {
            throw new RuntimeException("Collission in identifier name, please ensure that the slug for the identifiers is differents");
          }
        }
        catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
      else {
        throw new RuntimeException(identityFile + " is not a file!");
      }
    }
    else {
      throw new RuntimeException("directory " + directory.getPath() + " does not exist!");
    }
  }

  public byte[] retrieve(String identifier, int uniqueIdentifier)
  {
    String normalizedFilename = normalizeFileName(identifier);
    File directory = new File(basePath, normalizedFilename);
    if (directory.exists()) {
      File identityFile = new File(directory, "identity");
      if (identityFile.isFile()) {
        try {
          byte[] stored = Files.toByteArray(identityFile);
          if (Arrays.equals(stored, identifier.getBytes())) {
            File filename = new File(directory, String.valueOf(uniqueIdentifier));
            if (filename.exists() && filename.isFile()) {
              return Files.toByteArray(filename);
            }
            else {
              throw new RuntimeException("File " + filename.getPath() + " either is non existent or not a file!");
            }
          }
          else {
            throw new RuntimeException("Collission in identifier name, please ensure that the slug for the identifiers is differents");
          }
        }
        catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
      else {
        throw new RuntimeException(identityFile + " is not a file!");
      }
    }
    else {
      throw new RuntimeException("directory " + directory.getPath() + " does not exist!");
    }
  }

//
//  private File getNextFileName(String identifier, String filename)
//  {
//    String normalizedFileName = normalizeFileName(identifier);
//    File directory = new File(basePath, normalizedFileName);
//    if (directory.isDirectory()) {
//      /* make sure that it's the directory for the current identifier */
//      }
//    }
//
//    return null;
//  }
//
//  public Block retrieveFirstBlock(final String identifier)
//  {
//    final File file = getNextFileName(identifier, "");
//    return retrieveBlock(file, identifier);
//  }
//
//  public Block retrieveNextBlock(Block block)
//  {
//    final File file = getNextFileName(block.getIdentifier(), block.getNumber());
//    return retrieveBlock(file, block.getIdentifier());
//  }
//
//  @Override
//  public Block storeFirstBlock(final String identifier, byte[] bytes, int startingOffset, int endingOffset)
//  {
//  }
//
//  public Block delete(Block block)
//  {
//    String normalizedFileName = normalizeFileName(block.getIdentifier());
//    File directory = new File(basePath, normalizedFileName);
//    if (directory.exists()) {
//      final File file = new File(directory, block.getNumber());
//      if (file.exists()) {
//        if (file.delete()) {
//          return block;
//        }
//      }
//    }
//
//    return null;
//  }
//
//  public void write(File file, byte[] bytes) throws IOException
//  {
//    Files.write(bytes, file);
//  }
//
//  public byte[] read(File file) throws IOException
//  {
//    ByteArrayOutputStream ous = new ByteArrayOutputStream();
//    InputStream ios = new FileInputStream(file);
//    try {
//      byte[] buffer = new byte[4096];
//      int read;
//      while ((read = ios.read(buffer)) != -1) {
//        ous.write(buffer, 0, read);
//      }
//    }
//    finally {
//      try {
//        if (ous != null) {
//          ous.close();
//        }
//      }
//      catch (IOException e) {
//      }
//
//      try {
//        if (ios != null) {
//          ios.close();
//        }
//      }
//      catch (IOException e) {
//      }
//    }
//    return ous.toByteArray();
//  }
//
//  protected Block retrieveBlock(final File file, final String identifier)
//  {
//    try {
//      final byte[] contents = read(file);
//      return new Block()
//      {
//        public String getIdentifier()
//        {
//          return identifier;
//        }
//
//        public String getNumber()
//        {
//          return file.getName();
//        }
//
//        public byte[] getBytes()
//        {
//          return contents;
//        }
//
//      };
//    }
//    catch (IOException ex) {
//    }
//
//    return null;
//  }
//
//  /**
//   *
//   * @param block
//   * @param bytes
//   * @param startingOffset
//   * @param endingOffset
//   * @return
//   */
//  public Block storeNextBlock(Block block, byte[] bytes, int startingOffset, int endingOffset)
//  {
//    String normalizedFileName = normalizeFileName(block.getIdentifier());
//    File directory = new File(basePath, normalizedFileName);
//    if (directory.exists()) {
//      int i = Integer.parseInt(block.getNumber());
//      return writeFile(bytes, startingOffset, endingOffset, directory, block.getIdentifier(), String.valueOf(i + 1));
//    }
//
//    return null;
//  }
  protected int writeFile(byte[] bytes, int startingOffset, int endingOffset, File directory, final int number)
  {
    try {
      final byte[] newbytes;
      if (startingOffset > 0 || endingOffset < bytes.length) {
        newbytes = new byte[endingOffset - startingOffset];
        System.arraycopy(bytes, startingOffset, newbytes, 0, endingOffset - startingOffset);
      }
      else {
        newbytes = bytes;
      }
      Files.write(newbytes, new File(directory, String.valueOf(number)));
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    return number;
  }

}
