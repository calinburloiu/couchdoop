package com.avira.bdo;

import static org.junit.Assert.*;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class PlaygroundTest {

  private InputStream is = null;

  public void openFile(String fileName) throws IOException {
    try {
      is = new FileInputStream(fileName);
      if (!fileName.isEmpty()) {
//        throw new IOException("artificial exception");
      }
      System.out.println("hello!");
    } finally {
      System.out.println("finally in openFile");
    }
  }

  @Test
  public void test() {
    System.out.println("entering");

    try {
      System.out.println("opening");
      openFile("pom.xml");
    } catch (IOException e) {
      System.err.println("error in com.avira.bdo.chc.test: " + e.getMessage());
    } finally {
      System.out.println("finally in com.avira.bdo.chc.test");
      try {
        if (is != null) {
          System.out.println("closing");
          is.close();
        }
      } catch (IOException e) {
        System.out.println("error closing");
      }
    }

    assertTrue(true);
  }
}
