/* ============================================================================
*
* FILE: SocksClient.java
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

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

class ServerTest implements Closeable{

  private final SocketChannel channel;
  private ByteBuffer buff;
  /**
   * 
   * @param host
   * @param port
   * @throws IOException
   */
  public ServerTest(String host, int port) throws IOException
  {
    channel = SocketChannel.open(new InetSocketAddress(host, port));
  }
    
  private String requestResponseInBytes(int i, double d, String s) throws IOException
  {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream writer = new DataOutputStream(out);
    
    int len = 16 + s.toCharArray().length*2;
    
    try
    {
      writer.writeInt(len);
      writer.writeInt(i);
      writer.writeDouble(d);
      writer.writeInt(s.toCharArray().length);
      writer.writeChars(s);
      
      writer.flush();
      
      channel.write(ByteBuffer.wrap(out.toByteArray()));
      ByteBuffer buff = ByteBuffer.allocate(1024);
      int read = channel.read(buff);
      
     String r = new String(Arrays.copyOfRange(buff.array(), 0, read), StandardCharsets.UTF_8);
     return r; 
    }
    finally
    {
      try {
        writer.close();
      } catch (Exception e) {
        
      }
      
    }
  }
  
  static void loadTest(int iteration, String host, int port)
  {
    System.out.println("Connecting..");
    ServerTest [] clients = new ServerTest[iteration];
    for(int i = 0; i<iteration; i++)
    {
      try {
        clients[i] = new ServerTest(host, port);
      } catch (IOException e) {
        System.err.println("connect failed ["+i+"] => "+e.getMessage());
      }
    }
    for(int i = 0; i<iteration; i++)
    {
      ServerTest c = clients[i];
      try {
        
        if(c != null)
        {
          String msg = "Sending: "+i;
          //System.out.println("SENT: "+msg+"\tRECV: "+c.requestResponseInString(msg));
          System.out.println("SENT: "+msg+"\tRECV: "+c.requestResponseInBytes(i, i+0.5, msg));
          
        }
      } catch (IOException e) {
        System.err.println("req/res failed ["+i+"] -- stacktrace -- \n");
        e.printStackTrace();
      }
      finally
      {
        try {
          c.close();
        } catch (IOException e) {
          
        }
      }
    }
  }
  public static void main(String[] args) {
    System.out.println("-- Start run --");  
    long t1 = System.currentTimeMillis();
    //loadTest(1000, "localhost", 8092);
    
    int n = 400;
    System.out.println(new String(ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(n).array()));
    System.out.println("-- End run --");
    System.out.println("Time taken (ms): "+(System.currentTimeMillis()-t1));
  }
  @Override
  public void close() throws IOException {
    if (buff != null) {
      buff.clear();
    }
    channel.close();
  }

}
