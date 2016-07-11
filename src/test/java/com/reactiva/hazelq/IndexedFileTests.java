/* ============================================================================
*
* FILE: IndexedFileTests.java
*
The MIT License (MIT)

Copyright (c) 2016 Sutanu Dalui

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*
* ============================================================================
*/
package com.reactiva.hazelq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.reactiva.hazelq.db.MappedFile;

public class IndexedFileTests {

  private static void test(MappedFile ifile)
  {

    try {
      byte[] b = ifile.read("two");
        System.out.println(Thread.currentThread().getName() + " => " + new String(b, StandardCharsets.UTF_8));
        
        /*b = ifile.read("3");
        System.out.println(Thread.currentThread().getName() + " => " + new String(b, StandardCharsets.UTF_8));*/
        
        b = ifile.read("4");
        System.out.println(Thread.currentThread().getName() + " => 4" + b);
        
        ifile.remove("3");
        
        
        b = ifile.read("3");
        System.out.println(Thread.currentThread().getName() + " => 3 deleted "+b);

        b = ifile.read("two");
        System.out.println(Thread.currentThread().getName() + " => " + new String(b, StandardCharsets.UTF_8));
        
        b = ifile.read("one");
        System.out.println(Thread.currentThread().getName() + " => " + new String(b, StandardCharsets.UTF_8));
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
  
  }
  public static void main(String[] args) throws IOException {

    final MappedFile ifile = new MappedFile("C:\\data\\hazelq", "test.db");
    try
    {
      /*ifile.write("one", "one".getBytes(StandardCharsets.UTF_8));
      ifile.write("two", "two".getBytes(StandardCharsets.UTF_8));
      ifile.write("3", "3".getBytes(StandardCharsets.UTF_8));*/
      
      test(ifile);
      
      ExecutorService t = Executors.newFixedThreadPool(2);
            
      for (int i = 0; i < 3; i++) {
        t.submit(new Runnable() {

          @Override
          public void run() {
            test(ifile);

          }
        });
      }
            
      t.shutdown();
      try {
    t.awaitTermination(10, TimeUnit.MINUTES);
  } catch (InterruptedException e) {
    
  }
      
    }
    finally
    {
      ifile.close();
    }
    }

}
