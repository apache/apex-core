/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.storage;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class DiskStorage implements Storage
{
  private static final Logger logger = LoggerFactory.getLogger(DiskStorage.class);
  final String basePath;
  int uniqueIdentifier;

  public DiskStorage(String baseDirectory)
  {
    basePath = baseDirectory;
    logger.info("using {} as the basepath for spooling temporary files.", basePath);
  }

  public DiskStorage() throws IOException
  {
    File tempFile = File.createTempFile("msp", "msp");
    basePath = tempFile.getParent();
    tempFile.delete();
    logger.info("using {} as the basepath for spooling temporary files.", basePath);
  }

  public Storage getInstance() throws IOException
  {
    return new DiskStorage(basePath);
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
  public int store(String identifier, int uniqueIdentifier, byte[] bytes, int startingOffset, int endingOffset)
  {
    String normalizedFileName = normalizeFileName(identifier);
    File directory = new File(basePath, normalizedFileName);
    if (directory.exists()) {
      File identityFile = new File(directory, "identity");
      if (identityFile.isFile()) {
        try {
          byte[] stored = Files.toByteArray(identityFile);
          if (Arrays.equals(stored, identifier.getBytes())) {
            if (uniqueIdentifier == 0) {
              synchronized (this) {
                uniqueIdentifier = this.uniqueIdentifier++;
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

    return writeFile(bytes, startingOffset, endingOffset, directory, uniqueIdentifier);
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
