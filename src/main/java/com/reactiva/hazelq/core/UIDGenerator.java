/* ============================================================================
*
* FILE: UIDGenerator.java
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

import java.util.UUID;
import java.util.concurrent.locks.Lock;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISet;
import com.reactiva.hazelq.utils.TimeUIDGen;

public class UIDGenerator {

  @Autowired
  private HazelcastInstance hzService;
  private KryoPool kryoPool;
  @PostConstruct
  private void init()
  {
    setKryoPool(new KryoPool.Builder(new KryoFactory() {
      
      @Override
      public Kryo create() {
        Kryo kryo = new Kryo();
        // configure kryo instance, customize settings
        return kryo;
      }
    }).softReferences().build());
  }
  /**
   * Fetch (and generate if needed) next cluster wide unique time based ID.
   * @return a type 1 UUID
   * @throws InterruptedException
   */
  public UUID getNextUID() throws InterruptedException
  {
    IQueue<UUID> uidQ = hzService.getQueue(UIDGenerator.class.getName());
    UUID uid = uidQ.poll();
    while(uid == null)
    {
      Lock lock = hzService.getLock(UIDGenerator.class.getName());
      if(lock.tryLock())
      {
        try
        {
          uid = uidQ.poll();
          if(uid == null)
          {
            uid = createNextBatch(uidQ);
          }
        }
        finally
        {
          lock.unlock();
        }
      }
      else
      {
        uid = uidQ.poll();
      }
    }
    
    return uid;
  }

  @Value("${uid.batch.gen:100000}")
  private int genBatchSize;
  
  private static final Logger log = LoggerFactory.getLogger(UIDGenerator.class);
  //this is synchronized
  private UUID createNextBatch(IQueue<UUID> uidQ) {
    ISet<Long> lastTS = hzService.getSet(UIDGenerator.class.getName());
    long startOffset;
    if(lastTS.isEmpty())
    {
      startOffset = System.currentTimeMillis();
    }
    else
    {
      startOffset = lastTS.iterator().next();
    }
    
    log.info("createNextBatch with startOffset: "+startOffset);
    for(int i=0;i<genBatchSize;i++)
    {
      UUID u = TimeUIDGen.getTimeUUID(startOffset);
      uidQ.offer(u);
      
      startOffset++;
    }
    
    lastTS.clear();
    lastTS.add(startOffset);
    
    return uidQ.poll();
    
  }
  public KryoPool getKryoPool() {
    return kryoPool;
  }
  private void setKryoPool(KryoPool kryoPool) {
    this.kryoPool = kryoPool;
  }
}
