package com.avira.couchdoop.imp;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class ImportViewArgsTest {

  @Test
  public void splitViewKeysTest() {
    String[] tests = new String[]{
        "[\"20140720\",0];[\"20140720\",1];[\"20140720\",2];[\"20140720\",3]",
        "[\"201407;20\",0];[\"20140720\",1];[\"20140720\",2];[\"20140720\",3]",
        "[za;asa;sada];[za;asa;sada];[za;asa;sada];[za;asa;sada]",
        "[\"[201407;20\",0];[\"[201407;20\",1];[\"20140720\",2];[\"20140720\",3]",
    };

    for(String test: tests) {
      List<String> splits = ImportViewArgs.splitViewKeys(test);
      assertEquals("Number of splits is not correct!", 4, splits.size());
    }
  }


    @Test
    public void parseViewKeysTest() {
        String[] tests = new String[] {
                "[\"201407((01-31))\",0];[\"20140720\",1];[\"20140720\",2];[\"20140720\",3]",
        };

        for(String test: tests) {
            String[] keys = ImportViewArgs.parseViewKeys(test);
            System.out.println(Arrays.toString(keys));
            assertEquals("Number of keys is not correct!", 34, keys.length);
        }
    }


    @Test
    public void parseViewKeysTest2() {
        String keysString = "\"2014-07-07\";\"2014-07-08\"";

        String[] keys = ImportViewArgs.parseViewKeys(keysString);
        System.out.println(Arrays.toString(keys));
        assertEquals("Number of keys is not correct!", 2, keys.length);
    }



}
