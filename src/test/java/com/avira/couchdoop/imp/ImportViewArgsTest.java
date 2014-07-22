package com.avira.couchdoop.imp;

import com.avira.couchdoop.imp.ImportViewArgs;
import org.junit.Test;
import static org.junit.Assert.*;

public class ImportViewArgsTest {

  @Test
  public void getViewKeysTest(){
    String[] tests = new String[]{
        "[\"20140720\",0];[\"20140720\",1];[\"20140720\",2];[\"20140720\",3]",
        "[\"201407;20\",0];[\"20140720\",1];[\"20140720\",2];[\"20140720\",3]",
        "[za;asa;sada];[za;asa;sada];[za;asa;sada];[za;asa;sada]",
        "[\"[201407;20\",0];[\"[201407;20\",1];[\"20140720\",2];[\"20140720\",3]",
    };

    for(String test: tests) {
      String[] splits = ImportViewArgs.getViewKeys(test);
      assertEquals("Number of splits is not correct!", 4, splits.length);
    }
  }
}
