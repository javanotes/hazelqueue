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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.Assert;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.reactiva.hazelq.Message;

public class QueueService {

  private static final Logger log = LoggerFactory.getLogger(QueueService.class);
  @Autowired
  private HazelcastInstance hz;
  private Map<String, MQueueImpl> allQueue = new HashMap<>();
  private Map<String, QueueContainer> allQueueListeners = new HashMap<>();
  private ExecutorService containerThreads;
  @Value("${spring.hazelcast.tmp_path}")
  private String tmpFolder;
  
  @Autowired
  private UIDGenerator uidGen;
  /**
   * If hazelcast is running
   * @return
   */
  public boolean isRunning(){
    try {
      return hz.getLifecycleService().isRunning();
    } catch (Exception e) {
      //log.warn(e);
    }
    return false;
  }
  @PreDestroy
  private void stop()
  {
    log.info("::::::::: Shutdown sequence initiated ::::::::");
    for(MQueueImpl mq : allQueue.values())
    {
      mq.close();
    }
    for(QueueContainer qc : allQueueListeners.values())
    {
      qc.destroy();
    }
    containerThreads.shutdown();
    try {
      containerThreads.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      
    }
    if(hz.getLifecycleService().isRunning())
      hz.getLifecycleService().shutdown();
  }
  
  static final String HZ_MAP_SERVICE = "hz:impl:mapService";
  private DefaultMigrationListener migrationListener;
  private int containerThreadCount = 0;
  @PostConstruct
  private void init()
  {
    containerThreads = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), new ThreadFactory() {
      
      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r, "Container - "+(containerThreadCount++));
        return t;
      }
    });
     
    log.info("Waiting for node to balance..");
    while(!hz.getPartitionService().isLocalMemberSafe()){
      
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        
      }
    }
    log.info("Initializing distributed objects..");
    
    migrationListener = new DefaultMigrationListener(hz);
    hz.getCluster().addMembershipListener(migrationListener);
    hz.getPartitionService().addMigrationListener(migrationListener);
    hz.getPartitionService().addPartitionLostListener(migrationListener);
    
    for(DistributedObject obj : hz.getDistributedObjects())
    {
      if(HZ_MAP_SERVICE.equals(obj.getServiceName()))
      {
        String imap = obj.getName();
        MQueueImpl mq = getQ(imap);
        
        
        for(Partition part : hz.getPartitionService().getPartitions())
        {
          if(part.getOwner().localMember())
            mq.fireOnMigration(part.getPartitionId());
        }
        
        log.info("Registered queue => "+imap);
      }
      
    }
    
    log.info("Hazelcast system initialized. Joined group ["+hz.getConfig().getGroupConfig().getName()+"]");
    StringBuilder sb = new StringBuilder("");
    for(Member m : hz.getCluster().getMembers())
    {
      sb.append("\n\t").append(m.toString());
    }
    
    log.info(sb.toString());
    log.info("::::::::: Startup sequence completed ::::::::");
    
  }
  public QueueService(){
    
  }
  /**
   * Register listener on a given queue.
   * @param ql
   * @param q
   */
  public void registerListener(QueueListener ql, String q)
  {
    getL(q).register(ql);
  }
  private QueueContainer getL(String q)
  {
    if(!allQueueListeners.containsKey(q))
    {
      synchronized (allQueueListeners) {
        if(!allQueueListeners.containsKey(q))
        {
          QueueContainer qc = new QueueContainer(q, this);
          qc.setThreadPool(containerThreads);
          qc.setTempDBPath(tmpFolder);
          qc.start();
          allQueueListeners.put(q, qc);
        }
      }
    }
    
    return allQueueListeners.get(q);
  }
  private MQueueImpl getQ(String q)
  {
    if(!allQueue.containsKey(q))
    {
      synchronized (allQueue) {
        if(!allQueue.containsKey(q))
        {
          MQueueImpl dq = new MQueueImpl(hz, q);
          dq.setUidGen(uidGen);
          migrationListener.registerQueue(dq);
          allQueue.put(q, dq);
        }
      }
    }
    
    return allQueue.get(q);
  }
  /**
   * 
   * @param m
   * @param q
   * @return
   */
  private boolean add(QMessage m, String q)
  {
    return getQ(q).add(m);
  }
  
  /**
   * Adds a new message to queue.
   * @param msg
   * @return
   */
  public boolean add(Message msg)
  {
    if(!isRunning())
      return false;
    Assert.notNull(msg.getDestination(), "Destination is null");
    return add(new QMessage(msg), msg.getDestination());
  }
  /**
   * Non-Blocking poll.
   * @param q
   * @return
   * @throws InterruptedException
   */
  public MessageAndKey poll(String q)
  {
    return getQ(q).pollMessageAndKey();
    
  }
  
  /**
   * Will wait for a finite amount of time.
   * @param q
   * @param duration
   * @param unit
   * @return
   * @throws InterruptedException
   */
  QMessage poll(String q, long duration, TimeUnit unit) throws InterruptedException
  {
    return getQ(q).poll(duration, unit);
    
  }
  /**
   * Uninterruptible version of {@link #poll(String, long, TimeUnit)}.
   * @param q
   * @param duration
   * @param unit
   * @return
   */
  QMessage pollUninterruptibly(String q, long duration, TimeUnit unit)
  {
    boolean interrupted = false;
    while (true) {
      try {
        return poll(q, duration, unit);
      } catch (InterruptedException e) {
        interrupted = true;
      } 
      finally{
        if(interrupted)
          Thread.currentThread().interrupt();
      }
    }
    
  }

  /**
   * 
   * @param q
   * @param duration
   * @param unit
   * @return
   * @throws InterruptedException
   */
  QMessage peek(String q, long duration, TimeUnit unit) throws InterruptedException
  {
    return getQ(q).peek(duration, unit);
    
  }
  /**
   * 
   * @param q
   * @return
   */
  public Integer size(String q)
  {
    return getQ(q).size();
    
  }
  /**
   * 
   * @param q
   */
  public void clear(String q)
  {
    getQ(q).clear();
  }
  /**
   * Do a redelivery.
   * @param m
   */
  void add(MessageAndKey m, boolean redelivery) {
    if(redelivery)
      m.message.incrRedelivery();
    getQ(m.message.getPayload().getDestination()).add(m.key, m.message);
  }
}