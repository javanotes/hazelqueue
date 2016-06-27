/* ============================================================================
*
* FILE: DefaultMigrationListener.java
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.partition.PartitionLostEvent;
import com.hazelcast.partition.PartitionLostListener;

class DefaultMigrationListener implements MigrationListener, PartitionLostListener, MembershipListener {
  private static final Logger log = LoggerFactory.getLogger(DefaultMigrationListener.class);
  private HazelcastInstance hzInstance;
  /**
   * 
   */
  
  /**
   * @param hazelcastClusterServiceBean
   */
  public DefaultMigrationListener(HazelcastInstance hazelcastClusterServiceBean) {
    this.hzInstance = hazelcastClusterServiceBean;
  }
  private final Set<Integer> partCounts = new HashSet<>();
  @Override
  public void migrationStarted(MigrationEvent migrationevent) {
    synchronized (partCounts) {
      if(partCounts.isEmpty())
      {
        for(MQueueImpl q : observers)
        {
          q.fireOnMigrationStart();
        }
        
      }
      partCounts.add(migrationevent.getPartitionId());
      partCounts.notifyAll();
    }
    
    
  }

  @Override
  public void migrationFailed(MigrationEvent migrationevent) {
    log.warn("** Partition migration failed ** "+migrationevent);
    
    doEndMigration(null, migrationevent.getPartitionId());
  }
  
  private final List<MQueueImpl> observers = new ArrayList<>();

  public void registerQueue(MQueueImpl dq)
  {
    observers.add(dq);
  }
  
  private void doEndMigration(MigrationEvent migrationevent, int partId)
  {
    synchronized (partCounts) {
      while(partCounts.isEmpty())
      {
        try {
          partCounts.wait();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      partCounts.remove(partId);
      if(partCounts.isEmpty())
      {
        for(MQueueImpl q : observers)
        {
          q.fireOnMigrationEnd();
        }
      }
    }
  }
  @Override
  public void migrationCompleted(MigrationEvent migrationevent) 
  {
    //log.info("Invoking MigratedEntryProcessors");
    log.debug("Invoking MigratedEntryProcessors. Migration detected for partition => "+migrationevent.getPartitionId());
    doEndMigration(migrationevent, migrationevent.getPartitionId());
    
  }

  @Override
  public void partitionLost(PartitionLostEvent event) {
    log.info("PartitionLostEvent. Migration away detected of partition => "+event.getPartitionId());
  }

  @Override
  public void memberAdded(MembershipEvent membershipEvent) {
    log.info("Pausing consume on member added :: "+membershipEvent);
    synchronized (partCounts) {
      
      for(MQueueImpl q : observers)
      {
        q.pause();
      }
    }
    while(!hzInstance.getPartitionService().isMemberSafe(membershipEvent.getMember()))
    {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        
      }
    }
    
    synchronized (partCounts) {
      
      for(MQueueImpl q : observers)
      {
        q.resume();
      }
    }

    log.info("Member balance ready. Resuming..");
  }

  @Override
  public void memberRemoved(MembershipEvent membershipEvent) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void memberAttributeChanged(
      MemberAttributeEvent memberAttributeEvent) {
    // TODO Auto-generated method stub
    
  }

}