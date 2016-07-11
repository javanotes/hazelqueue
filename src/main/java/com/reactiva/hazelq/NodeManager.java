/* ============================================================================
*
* FILE: NodeManager.java
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

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;

import com.reactiva.hazelq.core.IQueueService;
import com.reactiva.hazelq.core.QueueService;
import com.reactiva.hazelq.core.UIDGenerator;
import com.reactiva.hazelq.net.ProtocolHandlerFactory;
import com.reactiva.hazelq.net.ServerSocketListener;

@SpringBootApplication
public class NodeManager {

  @Value("${server.port:6985}")
  private int port;
  @Value("${server.io-threads:1}")
  private int ioThreads;
  @Value("${server.event-threads.max}")
  private int evtThreadsMax;
  @Value("${server.event-threads.min}")
  private int evtThreadsMin;
  
  @Bean
  public ProtocolHandlerFactory handlerFactory()
  {
    return new ProtocolHandlerFactory();
  }
  @Bean
  public IQueueService service()
  {
    return new QueueService();
  }
  @Bean
  @DependsOn({"service"})
  public UIDGenerator uidgen()
  {
    return new UIDGenerator();
  }
  @Bean
  @DependsOn({"uidgen"})
  public ServerSocketListener listener() throws IOException
  {
    return new ServerSocketListener(port, evtThreadsMax, evtThreadsMin, ioThreads);
  }
  public static void main(String[] args) {
    SpringApplication.run(NodeManager.class, args);
  }
  @PostConstruct
  private void init() throws IOException
  {
    listener().startServer();
  }
  @PreDestroy
  private void destroy() throws IOException
  {
    listener().stopServer();
  }
}
