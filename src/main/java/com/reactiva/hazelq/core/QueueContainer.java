/* ============================================================================
*
* FILE: QueueContainer.java
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
package com.reactiva.hazelq.core;

import java.util.LinkedList;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.reactiva.hazelq.utils.Synchronizer;

class QueueContainer implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(QueueContainer.class);
  private boolean running = false;
  private final String qname;
  private long timeout = 1000;
  private TimeUnit timeoutUnit = TimeUnit.MILLISECONDS;
  
  private final QueueService service;
  
  public QueueContainer(String qname, QueueService service) {
    this.qname = qname;
    this.service = service;
  }
  
  /**
   * 
   */
  public void start()
  {
    running = true;
    service.submitPoller(this);
  }
    
  public void destroy()
  {
    running = false;
  }
  /**
   * Register a new {@linkplain QueueListener} with callback method {@linkplain QueueListener#onMessage(QMessage) onMessage} 
   * to be invoked on a new message placed in the queue.
   * @param o
   */
  public void register(QueueListener o)
  {
    addObserver(o);
  }
  /**
   * 
   * @param o
   * @return
   */
  public boolean unregister(QueueListener o)
  {
    sync.begin();
    try {
      return list.remove(o);
    } finally {
      sync.end();
    }

  }
  private final LinkedList<QueueListener> list = new LinkedList<>();
  private final Synchronizer sync = new Synchronizer();
  /**
   * 
   * @param o
   */
  private void addObserver(QueueListener o) {
    sync.begin();
    try {
      list.add(o);
    } finally {
      sync.end();
    }
    
  }
  private QueueListener nextObserver()
  {
    QueueListener o;
    sync.begin();
    try {
      o = list.pollFirst();
    }
    finally
    {
      sync.end();
    }
    return o;
  }
  private void run0()
  {

    QueueListener o = nextObserver();
    if(o != null)
    {
      try 
      {
        service.submitWorker(new Worker(o, o.concurrency()));
      } finally {
        addObserver(o);
        service.submitPoller(this);  
      }
    }
          
  
  }
  @Override
  public void run() {
    if(running)
    {
      run0();
    }

  }
  /**
   * Poll for next message.
   * @return
   */
  protected MessageAndKey pollNext()
  {
    return service.pollUninterruptibly(qname, timeout, timeoutUnit);
    //return service.poll(qname);
  }
  /**
   * 
   */
  private class Worker extends RecursiveAction
  {

    private final int concurrency;
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    /**
     * 
     * @param o
     */
    public Worker(QueueListener o, int concurrency) {
      super();
      this.o = o;
      this.concurrency = concurrency;
    }

    private final QueueListener o;

    private void run() 
    {
      if (running) 
      {
        MessageAndKey m = pollNext();
        log.debug("Polling done..");
        if (m.message != null) {

          if(running)
            notifyObserver(m, o);
          else
            service.add(m, false);
        } 

      }
     }

    @Override
    protected void compute() {
      if(concurrency > 1)
      {
        //fork
        for(int i=1; i<o.concurrency(); i++)
        {
          new Worker(o, 1).fork();
        }
      }
      else
      {
        //do yourself
        run();
      }
    }
    
  }
  private void notifyObserver(final MessageAndKey m, final QueueListener o) 
  {
    try 
    {
      o.update(null, m);
      service.commit(m);
    } catch (Exception e) {
      log.error("-- Container caught execption --", e);
      service.add(m, true);
    }
  
  }
 
  
  public long getTimeout() {
    return timeout;
  }
  public void setTimeout(long timeout) {
    this.timeout = timeout;
  }
  public TimeUnit getTimeoutUnit() {
    return timeoutUnit;
  }
  public void setTimeoutUnit(TimeUnit timeoutUnit) {
    this.timeoutUnit = timeoutUnit;
  }
  
}
