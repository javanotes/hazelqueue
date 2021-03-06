/* ============================================================================
*
* FILE: MQueueImpl.java
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

import java.io.Closeable;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.reactiva.hazelq.grid.AbstractLocalMapEntryListener;
import com.reactiva.hazelq.utils.Synchronizer;
/**
 * 
 *
 * @param <E>
 */
class MQueueImpl extends AbstractLocalMapEntryListener<QMessage> implements MQueue,Closeable {

  
  private UIDGenerator uidGen;
  private final String queueName;
  
  
  private ILock qLock;
  private ISet<QID> headSet;
  private final IMap<QID, QMessage> qMap;
  
  private IMap<QID, QMessage> qMap2;
  
  /**
   * 
   * @param hzService
   * @param queueName
   */
  public MQueueImpl(HazelcastInstance hzService, String queueName) {
    super(hzService);
    this.queueName = queueName;
    qLock = hzService.getLock(queueName);
    headSet = hzService.getSet(queueName);
    qMap = hzService.getMap(queueName);
    qMap2 = hzService.getMap(queueName+QueueService.QMAP_SUFFIX);
    
    headSet.addItemListener(new ItemListener<QID>() {
      
      @Override
      public void itemRemoved(ItemEvent<QID> item) {
        
      }
      
      @Override
      public void itemAdded(ItemEvent<QID> item) {
        synchronized (mutex) {
          mutex.notifyAll();
        }
        
      }
    }, false);
    
    
    register();
    
  }

  public IMap<QID, QMessage> getqMap() {
    return qMap;
  }
  
  /* (non-Javadoc)
   * @see com.reactiva.hazelq.core.MQueue#clear()
   */
  @Override
  public void clear() {
    qMap.clear();
  }

  /* (non-Javadoc)
   * @see com.reactiva.hazelq.core.MQueue#size()
   */
  @Override
  public int size() {
    return qMap.size();
  }

  @Override
  public String keyspace() {
    return queueName;
  }

  @Override
  public void entryAdded(EntryEvent<QID, QMessage> event) {
    addHead();
  }
  void pause()
  {
    while(!pauseSignalled.compareAndSet(false, true));
  }
  void resume()
  {
    pauseSignalled.compareAndSet(true, false);
  }
  private void qLockUninterruptibly()
  {
    boolean interrupted = false;
    try {
      while(!qLock.tryLock(100, TimeUnit.MILLISECONDS));
    } catch (InterruptedException e1) {
      interrupted = true;
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }
  //synchronized block
  private boolean addHead0()
  {
    if(pauseSignalled.get())
      return false;
    TreeSet<QID> set = new TreeSet<>();
    set.addAll(qMap.localKeySet());
    if (!set.isEmpty()) {
      QID next = set.first();
      return headSet.add(next);
    }
    return false;
  }
  private Synchronizer sync = new Synchronizer();
  private boolean addHead()
  {
    sync.begin();
    if(!stopping.compareAndSet(false, false)){
      sync.end();
      return false;
    }
    qLockUninterruptibly();
    try 
    {
      return addHead0();
      
    } catch (Exception e) {
      e.printStackTrace();
    }
    finally
    {
      qUnlock();
      sync.end();
    }
    
    return false;
  }

  @Override
  public void entryRemoved(EntryEvent<QID, QMessage> event) {
    addHead();
  }

  private QID removeHead() throws InterruptedException
  {
    if(!hzService.getLifecycleService().isRunning())
      return null;
    qLockUninterruptibly();
    try 
    {
      if(pauseSignalled.get())
        return null;
      TreeSet<QID> set = new TreeSet<>(headSet);
      if(!set.isEmpty()){
        QID head = set.first();
        if(headSet.remove(head)){
          return head;
        }
      }
    } finally {
      qUnlock();
    }
    return null;
    
  }
  
  /* (non-Javadoc)
   * @see com.reactiva.hazelq.core.MQueue#add(com.reactiva.hazelq.core.QMessage)
   */
  @Override
  public boolean add(QMessage item) {
    try 
    {
      UUID u = uidGen.getNextUID();
      add(new QID(u), item);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return false;
  }
  /**
   * 
   * @param key
   * @param val
   */
  public void add(QID key, QMessage val)
  {
    qMap.set(key, val);
  }
  private static final Logger log = LoggerFactory.getLogger(MQueueImpl.class);
  /**
   * @deprecated
   * @param k
   * @return
   * @throws InterruptedException
   */
  private QMessage peek0(QID k) throws InterruptedException
  {
    QID key = k != null ? k : removeHead();
    if (key != null) {
      return read(false, key);
    }
    return null;
  }
  /** (non-Javadoc)
   * @see com.reactiva.hazelq.core.MQueue#peek()
   * @deprecated Not supported
   */
  @Override
  public QMessage peek(long timeout, TimeUnit unit) throws InterruptedException {
    QMessage msg = null;
    try 
    {
      QID mk;
      synchronized (mutex) 
      {
        mk = removeHead();
        if (mk == null) {
          mutex.wait(unit.toMillis(timeout));
        }

      }

      msg = peek0(mk);

    } 
    catch (Exception e) {
      e.printStackTrace();
    }
    return msg;
  }
  void reEnqueueUnprocessed()
  {
    for(QID id : qMap2.localKeySet())
    {
      add(id, qMap2.remove(id));
    }
  }
  void removeSurrogate(QID key)
  {
    qMap2.removeAsync(key);
  }
  private QMessage read(boolean commit, QID key)
  {
    QMessage m = qMap.remove(key);
    if (m != null) {
      qMap2.set(key, m);
    }

    return m;
  }
  
  private final Object mutex = new Object();

  /* (non-Javadoc)
   * @see com.reactiva.hazelq.core.MQueue#poll()
   */
  @Override
  public MessageAndKey poll()  {
    MessageAndKey mk = pollMessageAndKey();
    return mk;
    
  }
  /**
   * 
   */
  @Override
  public MessageAndKey poll(long timeout, TimeUnit unit) throws InterruptedException {

    QID mk = null;
    MessageAndKey mkey = null;
    try 
    {
      mk = removeHead();
      if (mk == null) 
      {
        synchronized (mutex) 
        {
          mk = removeHead();
          if (mk == null) {
            if (timeout > 0) {
              mutex.wait(unit.toMillis(timeout));
            } else {
              mutex.wait();
            } 
          }
        }
      }
      mkey = poll0(mk);
    } 
    
    catch (Exception e) {
      e.printStackTrace();
    }
    return mkey;
  
  }

  /**
   * 
   * @return
   */
  private MessageAndKey pollMessageAndKey()  {
    QID mk = null;
    MessageAndKey mkey = null;
    try 
    {
      
      mk = removeHead();
      
      if (mk != null) {
        mkey = poll0(mk);
        
      }
      
    } 
    
    catch (Exception e) {
      e.printStackTrace();
    }
    
    return mkey;
  
  
  }
  
  private MessageAndKey poll0(QID k) throws InterruptedException {
    QID key = k != null ? k : removeHead();
    if (key != null) {
      return new MessageAndKey(key, read(true, key));
    }
    return new MessageAndKey(null, null);
  }

  //private final Lock partOpsLock = new ReentrantLock();
  
  public UIDGenerator getUidGen() {
    return uidGen;
  }

  public void setUidGen(UIDGenerator uidGen) {
    this.uidGen = uidGen;
  }
  /**
   * A new entry has migrated into this map
   * @param partId 
   * @param key
   */
  public void fireOnMigration(int partId) {
    addHead();
    log.debug("Handled migration of partition.. "+partId);
  }
  private final AtomicBoolean pauseSignalled = new AtomicBoolean();
  private void qUnlock()
  {
    try {
      qLock.unlock();
    } catch (Exception e) {
      
    }
  }
  public void fireOnMigrationStart() {
    qLockUninterruptibly();
    try
    {
      addHead0();
      try {
        Assert.isTrue(pauseSignalled.compareAndSet(false, true));
      } catch (Exception e) {
        log.warn("Assertion warning", e);
      }
    }
    finally
    {
      qUnlock();
    }
    log.debug("-- Partition migration begin --");
  }
  public void fireOnMigrationEnd() {
    qLockUninterruptibly();
    try
    {
      try {
        Assert.isTrue(pauseSignalled.compareAndSet(true, false));
      } catch (Exception e) {
        log.warn("Assertion warning", e);
      }
      addHead0();
    }
    finally
    {
      qUnlock();
    }
    log.debug("-- Partition migration end --");
    
  }


 
  
  private AtomicBoolean stopping = new AtomicBoolean();
  @Override
  public void close() {
    sync.begin();
    stopping.compareAndSet(false, true);
    sync.end();
  }
    
}
