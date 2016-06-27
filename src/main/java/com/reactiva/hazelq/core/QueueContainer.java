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
import java.util.Observer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class QueueContainer implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(QueueContainer.class);
  private boolean running = false;
  private final String qname;
  private long timeout = 10;
  private TimeUnit timeoutUnit = TimeUnit.MILLISECONDS;
  
  private final QueueService service;
  
  private ExecutorService threadPool;
  
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
    threadPool.submit(this);
  }
  
  public void destroy()
  {
    running = false;
  }
  public synchronized void register(Observer o)
  {
    addObserver(o);
    notify();
  }
  private final LinkedList<Observer> list = new LinkedList<>();
  /**
   * 
   * @param o
   */
  private void addObserver(Observer o) {
    list.add(o);
    
  }
  @Override
  public void run() {
    if(running)
    {
      
      if(countObservers() > 0)
      {
        QMessage m = service.poll(qname);
        log.debug("Polling done..");
        if (m != null) {
          notifyObserver(m);
        }
      }
                
      if(running)
        threadPool.submit(this);
    }

  }
  private Observer o;
  private final AtomicBoolean notifySync = new AtomicBoolean();
  private void notifyObserver(final QMessage m) 
  {
    while(!notifySync.compareAndSet(false, true));
    try 
    {
      o = list.pollFirst();
      threadPool.submit(new Runnable() {
        
        @Override
        public void run() {
          o.update(null, m);
        }
      });
      
    } finally {
      addObserver(o);
      notifySync.compareAndSet(true, false);
    }
    
  }
  private int countObservers() {
    return list.size();
  }

  public ExecutorService getThreadPool() {
    return threadPool;
  }

  public void setThreadPool(ExecutorService threadPool) {
    this.threadPool = threadPool;
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
