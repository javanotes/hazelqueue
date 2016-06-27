/* ============================================================================
*
* FILE: SocketConnection.java
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
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
class SocketConnector implements Closeable, Runnable{

  private static final Logger log = LoggerFactory.getLogger(SocketConnector.class);
  private SelectionKey selKey;
 
  private final SocketChannel channel;
  /**
   * 
   * @return
   */
  public SocketChannel getChannel() {
    return channel;
  }

  private final ProtocolHandler handler;
  private final int connId;
  /**
   * 
   * @param r2aBuff
   * @param channel
   * @throws IOException
   */
  SocketConnector(SocketChannel channel, ProtocolHandler handler) 
  {
    this.channel = channel;
    this.handler = handler;
    connId = hashCode();
    log.info("New socket connection# "+connId);
  }
  
  private DataInputStream inStream;
  
  /**
   * A stateless one time execution of a request/response flow with a remote client.
   * For stateful semantics, a session state should be managed at application level.
   * TODO: This method should be overridden as necessary.
   */
  private boolean read()
  {
    try 
    {
      boolean complete = handler.doRead(channel);
      if(complete){
        inStream = new DataInputStream(handler.getReadStream());
        
      }
      
      return complete;
      
    } catch (IOException e) {
      log.warn(e.getMessage());
      try {
        close();
      } catch (IOException e1) {
        log.warn(e.getMessage());
      }
    }
    return false;
    
  }
  
  private void runProcess()
  {
    try 
    {
      log.info("Executing connection# "+connId);
      writeBuff = ByteBuffer.wrap(handler.doProcess(inStream));
      selKey.interestOps(SelectionKey.OP_WRITE);
      selKey.selector().wakeup();
      
    } catch (Exception e) {
      log.error("Exception while processing connection# "+connId, e);
    }
  }
  private ByteBuffer writeBuff;
    
  private void doWrite()
  {
    try 
    {
      channel.write(writeBuff);
      if(!writeBuff.hasRemaining())
        close();
    } catch (IOException e) {
      log.warn(e.getMessage());
      try {
        close();
      } catch (IOException e1) {
        log.warn(e.getMessage());
      }
    }
  }
        
  @Override
  public void close() throws IOException {
    channel.finishConnect();
    channel.close();
    selKey.cancel();
    if (writeBuff != null) {
      writeBuff.clear();
      writeBuff = null;
    }
    handler.close();
    log.info("Connection closed# "+connId);
  }
  /**
   * submit response to processor threads.
   */
  void fireWrite() {
    doWrite();
  }
  
  /**
   * @return 
   * 
   */
  boolean fireRead() {
    return read();  
  }
  
  public void setSelKey(SelectionKey selKey) {
    this.selKey = selKey;
  }
  @Override
  public void run() {
    runProcess();
    
  }
  public int getConnId() {
    return connId;
  }
}
