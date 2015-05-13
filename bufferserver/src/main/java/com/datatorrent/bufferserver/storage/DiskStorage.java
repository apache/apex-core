/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.storage;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

/**
 * <p>DiskStorage class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public class DiskStorage implements Storage
{
  private static final Logger logger = LoggerFactory.getLogger(DiskStorage.class);
  final String basePath;
  int uniqueIdentifier;

  public DiskStorage(String baseDirectory)
  {
    basePath = baseDirectory;
    logger.info("Using {} as the basepath for spooling.", basePath);
  }

  public DiskStorage() throws IOException
  {
    File tempFile = File.createTempFile("msp", "msp");
    basePath = tempFile.getParent();
    tempFile.delete();
    logger.info("using {} as the basepath for spooling.", basePath);
  }

  @Override
  public Storage getInstance() throws IOException
  {
    return new DiskStorage(basePath);
  }

  public static String normalizeFileName(String name)
  {
    StringBuilder sb = new StringBuilder(1024);
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
    int lUniqueIdentifier;
    String normalizedFileName = normalizeFileName(identifier);
    File directory = new File(basePath, normalizedFileName);
    if (directory.exists()) {
      File identityFile = new File(directory, "identity");
      if (identityFile.isFile()) {
        try {
          byte[] stored = Files.toByteArray(identityFile);
          if (Arrays.equals(stored, identifier.getBytes())) {
            synchronized (this) {
              lUniqueIdentifier = ++this.uniqueIdentifier;
            }
          }
          else {
            throw new IllegalStateException("Collission in identifier name, please ensure that the slug for the identifiers is differents");
          }
        }
        catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
      else {
        throw new IllegalStateException("Identity file is hijacked!");
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

      lUniqueIdentifier = ++this.uniqueIdentifier;
    }

    try {
      return writeFile(bytes, startingOffset, endingOffset, directory, lUniqueIdentifier);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
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

  @Override
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
            throw new RuntimeException("Collision in identifier name, please ensure that the slugs for the identifiers [" + identifier + "], and [" +  new String(stored) + "] are different.");
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

  protected int writeFile(byte[] bytes, int startingOffset, int endingOffset, File directory, final int number) throws IOException
  {
    FileOutputStream stream = new FileOutputStream(new File(directory, String.valueOf(number)));
    try {
      stream.write(bytes, startingOffset, endingOffset - startingOffset);
    }
    finally {
      stream.close();
    }
    return number;
  }

}
