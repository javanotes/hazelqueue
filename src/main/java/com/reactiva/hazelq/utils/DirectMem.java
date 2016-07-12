/* ============================================================================
*
* FILE: DirectMem.java
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
package com.reactiva.hazelq.utils;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;

public class DirectMem {

  /**
   *   
   * @param bb
   * @return
   */
  public static boolean unmap(ByteBuffer bb)
  {
    /*
     * From  sun.nio.ch.FileChannelImpl
     * private static void  unmap(MappedByteBuffer bb) {
          
           Cleaner cl = ((DirectBuffer)bb).cleaner();
           if (cl != null)
               cl.clean();


       }
     */
    Method cleaner_method = null, clean_method = null;
    try 
    {
      if(cleaner_method == null)
      {
        cleaner_method = findMethod(bb.getClass(), "cleaner");
        cleaner_method.setAccessible(true);
      }
      if (cleaner_method != null) {
        Object cleaner = cleaner_method.invoke(bb);
        if (cleaner != null) {
          if (clean_method == null) {
            clean_method = findMethod(cleaner.getClass(), "clean");
            clean_method.setAccessible(true);
          }
          clean_method.invoke(cleaner);
          return true;
        } 
      }

    } catch (Throwable ex) 
    {
      //ignored   
    }
    return false;
  }
  
  private static Method findMethod(Class<?> class1, String method) throws NoSuchMethodException {
    for(Method m : class1.getDeclaredMethods())
    {
      if(m.getName().equals(method))
        return m;
    }
    throw new NoSuchMethodException(method);
  }

}
