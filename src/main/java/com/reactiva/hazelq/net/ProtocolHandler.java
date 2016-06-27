/* ============================================================================
*
* FILE: ProtocolHandler.java
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
package com.reactiva.hazelq.net;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.SocketChannel;
/**
 * A protocol message converter. 
 * @see AbstractProtocolHandler
 */
public interface ProtocolHandler extends Closeable{

  int BYTE_OFFSET = 1;
  int SHORT_OFFSET = 2;
  int INT_OFFSET = 4;
  int LONG_OFFSET = 8;
  /**
   * Get the input stream once read is complete.
   * @return
   */
  InputStream getReadStream();
  /**
   * Read from a given channel, and return true when read is complete.
   * @return if read is complete
   * @throws IOException
   */
  boolean doRead(SocketChannel channel) throws IOException;

  /**
   * Process the request and return bytes for response.
   * @param dataInputStream
   * @return
   */
  byte[] doProcess(DataInputStream dataInputStream) throws Exception;

  
}