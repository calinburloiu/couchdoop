package com.avira.bdo;

import static org.junit.Assert.*;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class Playground {

  private InputStream is = null;

  public void openFile(String fileName) throws IOException {
    try {
      is = new FileInputStream(fileName);
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

      System.out.println("closing");
      is.close();
    } catch (IOException e) {
      System.err.println("error in test");
    } finally {
      System.out.println("finally in test");
    }

    assertTrue(true);
  }
}
