/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.avira.couchdoop.imp;

import com.google.common.collect.Lists;
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

    for (String test : tests) {
      List<String> splits = ImportViewArgs.splitViewKeys(test);
      assertEquals("Number of splits is not correct!", 4, splits.size());
    }
  }


  @Test
  public void parseViewKeysTest() {
    String[] tests = new String[]{
        "[\"201407((01-31))\",0];[\"20140720\",1];[\"20140720\",2];[\"20140720\",3]",
    };

    for (String test : tests) {
      String[] keys = ImportViewArgs.parseViewKeys(test);
      System.out.println(Arrays.toString(keys));
      assertEquals("Number of keys is not correct!", 34, keys.length);
    }
  }

  @Test
  public void parseViewKeysPaddingTest() {
    String keysString = "2014-07-((08-12))";

    String[] keys = ImportViewArgs.parseViewKeys(keysString);
    assertEquals("Number of keys is not correct!", "2014-07-09", keys[1]);

  }

  @Test
  public void parseViewKeys3DigitPaddingTest() {
    String keysString = "[\"((000-127))\"]";
    String[] keys = ImportViewArgs.parseViewKeys(keysString);
    System.out.println(Lists.asList(" ", keys));
  }


  @Test
  public void parseViewKeysTest2() {
    String keysString = "\"2014-07-07\";\"2014-07-08\"";

    String[] keys = ImportViewArgs.parseViewKeys(keysString);
    System.out.println(Arrays.toString(keys));
    assertEquals("Number of keys is not correct!", 2, keys.length);
  }


}
