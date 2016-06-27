/* ============================================================================
*
* FILE: ServerSocketListener.java
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
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * A lightweight and fast, fixed length bytes protocol server, based on non-blocking NIO. Designed using a reactor pattern with a single thread for IO operations, 
 * and multiple threads for process execution.<p>Extend {@linkplain ProtocolHandlerFactory} to provide a 
 * custom {@linkplain ProtocolHandler} and override {@linkplain ProtocolHandler#doProcess(java.io.DataInputStream) doProcess()} 
 * and {@linkplain ProtocolHandler#doRead(SocketChannel) doRead()} methods. 
 * Register the factory using {@link #registerProtocolFactory(ProtocolHandlerFactory) registerProtocolFactory()} before invoking {@link #startServer()}.
 * <p>
 * 
 * @see ProtocolHandler
 */
public class ServerSocketListener implements Runnable{

  private static final Logger log = LoggerFactory.getLogger(ServerSocketListener.class);
  public static final int DEFAULT_READ_BUFF_SIZE = 32;
  
  private int port;

  // The channel on which we'll accept connections
  private ServerSocketChannel serverChannel;

  private Selector selector;
  
  private int minExecutorThreadCount, maxExecutorThreadCount, acceptorThreadCount;
  
  @Autowired
  private ProtocolHandlerFactory protoFactory;
  
  /**
   * 
   * @param port
   * @param readBufferSize
   * @param maxExecutorThreads
   * @param minExecutorThreads
   * @param acceptorThreads
   * @throws IOException
   */
  public ServerSocketListener(int port, int readBufferSize, int maxExecutorThreads, int minExecutorThreads, int acceptorThreads) throws IOException {
    this.port = port;
    this.maxExecutorThreadCount = maxExecutorThreads;
    this.minExecutorThreadCount = minExecutorThreads;
    this.acceptorThreadCount = acceptorThreads;
    open();
    
  }
  /**
   * 
   * @param port
   * @param maxExecutorThreads
   * @param minExecutorThreads
   * @param acceptorThreads
   * @throws IOException
   */
  public ServerSocketListener(int port, int maxExecutorThreads, int minExecutorThreads, int acceptorThreads) throws IOException {
    this(port, DEFAULT_READ_BUFF_SIZE, maxExecutorThreads, minExecutorThreads, acceptorThreads);
    
  }
  
  /**
   * Server running on given port and max thread count based on no of processors.
   * @param port
   * @throws IOException
   */
  public ServerSocketListener(int port) throws IOException {
    this(port, Runtime.getRuntime().availableProcessors());
    
  }
  /**
   * 
   * @param port
   * @param ioThread
   * @param maxThread
   * @throws IOException
   */
  public ServerSocketListener(int port, int maxThread) throws IOException {
    this(port, DEFAULT_READ_BUFF_SIZE, maxThread, 1, 1);
    
  }

  public static void main(String[] args) {
    try 
    {
      final ServerSocketListener loader = new ServerSocketListener(Integer.valueOf(args[0]));
      loader.startServer();
      Runtime.getRuntime().addShutdownHook(new Thread(){
        public void run()
        {
          loader.stopServer();
        }
      });
    } catch (Exception e) {
      e.printStackTrace();
      log.error("<Not started> ** "+e.getMessage()+" **");
    }
  }
  private void open() throws IOException {
    // Create a new selector
    selector = Selector.open();

    // Create a new non-blocking server socket channel
    serverChannel = ServerSocketChannel.open();
    serverChannel.configureBlocking(false);

    // Bind the server socket to the specified address and port
    InetSocketAddress isa = new InetSocketAddress(port);
    try {
      serverChannel.bind(isa);
    } catch (Exception e) {
      log.warn("Unable to start on provided port. Will select auto. Error => "+e.getMessage());
      serverChannel.bind(null);
      isa = (InetSocketAddress) serverChannel.getLocalAddress();
      port = isa.getPort();
    }

    // Register the server socket channel, indicating an interest in
    // accepting new connections
    serverChannel.register(selector, SelectionKey.OP_ACCEPT);

    
    ioThreadPool = Executors.newFixedThreadPool(acceptorThreadCount, new ThreadFactory() {
      
      private int threadCounter = 0;
      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r, "io-task-"+threadCounter++);
        return t;
      }
    });
    
    //execThreadPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
    
    execThreadPool = new ThreadPoolExecutor(minExecutorThreadCount, maxExecutorThreadCount, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(1024), new ThreadFactory() {
      private int threadCounter = 0;
      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r, "exec-task-"+threadCounter++);
        return t;
      }
    });
    
    acceptors = new LinkedList<>();
    for(int i=0; i<acceptorThreadCount; i++)
    {
      SocketAcceptor socks = new SocketAcceptor(execThreadPool);
      socks.setSeq(i);
      acceptors.add(socks);
    }
  }
 
  private ExecutorService execThreadPool;
  private LinkedList<SocketAcceptor> acceptors;
  
  private ExecutorService ioThreadPool;
  private boolean running;
  /**
   * 
   * @param key
   * @throws IOException
   */
  private void accept(SelectionKey key) throws IOException {

    SocketChannel socketChannel = serverChannel.accept();
    socketChannel.configureBlocking(false);
    SocketConnector conn = new SocketConnector(socketChannel, protoFactory.getObject());
    //round robin dispatch
    SocketAcceptor sa = acceptors.remove();
    try {
      sa.offer(conn);
    } finally {
      acceptors.addLast(sa);
    }
    
    
  }
  
  private void doSelect() throws IOException
  {

    // Wait for an event one of the registered channels
    selector.select();

    // Iterate over the set of keys for which events are available
    Set<SelectionKey> keySet = selector.selectedKeys();
    Iterator<SelectionKey> selectedKeys = keySet.iterator();
    
    while (selectedKeys.hasNext()) 
    {
      SelectionKey key = selectedKeys.next();
      
      if (!key.isValid()) {
        selectedKeys.remove();
        continue;
      }
      
      try 
      {
        if (key.isAcceptable()) {
          selectedKeys.remove();
          accept(key);
        }
        
      } catch (IOException e) {
        log.error("Exception on accepting new connection", e);
        
      }
      catch (RejectedExecutionException e) {
        log.warn("** Connection queue maxed out. Rejecting request **");
        log.debug("", e);
        disconnect(key);
      }
    }
  
  }
  
  private void disconnect(SelectionKey key) throws IOException 
  {
    ((SocketConnector) key.attachment()).close();
    
  }
  
  @Override
  public void run() 
  {
    running = true;
    log.info("TCP connector started on port ["+port+"] for incoming transport");
    while(running)
    {
      try {
        doSelect();
      } catch (IOException e) {
        log.error(" Unexpected exception in selector loop", e);
      }
    }
    stop();
  }
  
  private void stop() {
    try {
      selector.close();
    } catch (IOException e) {
      
    }
    for(SocketAcceptor socka : acceptors)
    {
      socka.stop();
    }
    try {
      serverChannel.close();
    } catch (IOException e) {
      
    }
    ioThreadPool.shutdown();
    try {
      ioThreadPool.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      
    }
    execThreadPool.shutdown();
    try {
      execThreadPool.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      
    }
    log.info("Stopped transport on port ["+port+"]");
  }
  

  /**
   * Stops the server.
   */
  public void stopServer()
  {
    running = false;
    selector.wakeup();
  }

  
  
  /**
   * Starts the server
   */
  public void startServer() 
  {
    for(SocketAcceptor socka : acceptors)
    {
      ioThreadPool.submit(socka);
    }
    new Thread(this, "listener-task").start();
  }
    
}
