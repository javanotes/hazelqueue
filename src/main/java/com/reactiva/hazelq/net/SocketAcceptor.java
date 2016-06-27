/* ============================================================================
*
* FILE: SocketAcceptor.java
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

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SocketAcceptor implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(SocketAcceptor.class);

  public static final int DEFAULT_MAX_QUEUED_CONN = 1024;
  
  // The selector we'll be monitoring
  private final Selector selector;
  private volatile boolean running = false;
  

  protected int threadCounter = 1;
  private BlockingQueue<SocketConnector> queuedConnections;
  private final ExecutorService execThreadPool;
  /**
   * 
   * @param conn
   * @throws IOException
   */
  public void offer(SocketConnector conn) throws IOException
  {
    try {
      if(!queuedConnections.offer(conn, 1000, TimeUnit.MILLISECONDS)){
        throw new RejectedExecutionException();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    selector.wakeup();
    
  }
  /**
   * 
   * @param execThreadPool
   * @param maxQueuedConn
   * @throws IOException
   */
  public SocketAcceptor(ExecutorService execThreadPool, int maxQueuedConn) throws IOException {
    this.selector = Selector.open();
    this.execThreadPool = execThreadPool;
    queuedConnections = new ArrayBlockingQueue<>(maxQueuedConn);
  }
  private int seq;
  /**
   * 
   * @param maxThread
   * @param minThread
   * @throws IOException
   */
  public SocketAcceptor(ExecutorService execThreadPool) throws IOException {
    this(execThreadPool, DEFAULT_MAX_QUEUED_CONN);
  }
  public void stop()
  {
    running = false;
    selector.wakeup();
        
  }
  /**
   * 
   * @param key
   * @throws IOException
   */
  private void read(SelectionKey key) throws IOException {
    
    SocketChannel channel = ((SocketChannel) key.channel());
    SocketConnector conn = (SocketConnector) key.attachment();
    
    try 
    {
     
      if (channel.isOpen()) {
        if(conn.fireRead())
        {
          execThreadPool.submit(conn);
        }
      }
      else
      {
        disconnect(key);
        log.info("Remote client disconnected..");
      }
      
    } catch (IOException e) {
      log.warn("Remote connection force closed", e);
      disconnect(key);
    }
    
  }
  
  private void disconnect(SelectionKey key) throws IOException 
  {
    ((SocketConnector) key.attachment()).close();
    
  }
  
  private void write(SelectionKey key) throws IOException 
  {
    SocketChannel channel = ((SocketChannel) key.channel());
    SocketConnector conn = (SocketConnector) key.attachment();
    try 
    {
     
      if (channel.isOpen()) {
        conn.fireWrite();
      }
      else
      {
        disconnect(key);
        log.info("Remote client disconnected..");
      }
      
    } catch (IOException e) {
      log.warn("Remote connection force closed", e);
      disconnect(key);
    }
    
  }
  
  private void doSelect() throws IOException
  {

    // Wait for an event one of the registered channels
    selector.select();

    // Iterate over the set of keys for which events are available
    Set<SelectionKey> keySet = selector.selectedKeys();
    Iterator<SelectionKey> selectedKeys = keySet.iterator();
    
    while (selectedKeys.hasNext()) {
      SelectionKey key = selectedKeys.next();
      
      if (!key.isValid()) {
        selectedKeys.remove();
        disconnect(key);
        continue;
      }
      
      // Check what event is available and deal with it
      try {
        
        if (key.isValid() && key.isReadable()) {
          selectedKeys.remove();
          read(key);
        }
        else if (key.isValid() && key.isWritable()) {
          selectedKeys.remove();
          write(key);
        }
      } catch (IOException e) {
        log.warn("Ignoring exception and removing connection", e);
        disconnect(key);
      }
    }
  
  }
  
  private void accept(SocketConnector conn) throws IOException
  {
    SocketChannel channel = conn.getChannel();
    conn.setSelKey(channel.register(selector, SelectionKey.OP_READ, conn));
    log.info("Accepted connection from remote host "+channel.getRemoteAddress());
  }
  @Override
  public void run() {
    running = true;
    log.info("Acceptor starting .."+getSeq());
    while (running) 
    {
      SocketConnector conn = queuedConnections.poll();
      if(conn != null)
      {
        try {
          accept(conn);
        } catch (IOException e) {
          log.error("Error while trying to accept new connection => "+e.getMessage());
          try {
            conn.close();
          } catch (IOException e1) {
            
          }
        }
      }
      try 
      {
        doSelect();
      } 
      catch (Exception e) {
        log.error("Unexpected exception in selector loop", e);
      }
    
    }
    
    log.info("Stopped acceptor .."+getSeq());

  }
  public int getSeq() {
    return seq;
  }
  public void setSeq(int seq) {
    this.seq = seq;
  }

}
