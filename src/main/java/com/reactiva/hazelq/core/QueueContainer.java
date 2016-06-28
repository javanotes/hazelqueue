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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.mapdb.serializer.SerializerByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.reactiva.hazelq.Message;
import com.reactiva.hazelq.utils.Synchronizer;
import com.reactiva.hazelq.utils.TimeUIDGen;

class QueueContainer implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(QueueContainer.class);
  private boolean running = false;
  private final String qname;
  private long timeout = 10;
  private TimeUnit timeoutUnit = TimeUnit.MILLISECONDS;
  
  private final QueueService service;
  
  private ExecutorService threadPool;
  private HTreeMap<QID, QMessage> mapdb;
  public QueueContainer(String qname, QueueService service) {
    this.qname = qname;
    this.service = service;
  }
  
  private void mapdb(File f)
  {
    DB db = DBMaker.fileDB(f).closeOnJvmShutdown().fileMmapEnableIfSupported().make();
    mapdb = db.hashMap(qname, new Serializer<QID>() {

      @Override
      public int compare(QID arg0, QID arg1) {
        return arg0.compareTo(arg1);
      }

      @Override
      public void serialize(DataOutput2 out, QID value) throws IOException {
        byte[] b = TimeUIDGen.decompose(value.getUid());
        out.writeInt(b.length);
        out.write(b);
      }

      @Override
      public QID deserialize(DataInput2 in, int available)
          throws IOException {
        byte[] b = new byte [in.readInt()];
        in.readFully(b);
        return new QID(TimeUIDGen.getUUID(ByteBuffer.wrap(b)));
      }
    }, new Serializer<QMessage>() {

      @Override
      public int compare(QMessage o1, QMessage o2) {
        return new SerializerByteArray().compare(o1.getPayload().getPayload(), o2.getPayload().getPayload());
      }

      @Override
      public void serialize(DataOutput2 out, QMessage value)
          throws IOException {
        if(value.getPayload() == null)
        {
          out.writeBoolean(false);
        }
        else
        {
          out.writeBoolean(true);
          out.writeUTF(value.getPayload().getCorrelationID());
          out.writeUTF(value.getPayload().getDestination());
          out.writeUTF(value.getPayload().getReplyTo());
          out.writeLong(value.getPayload().getExpiryMillis());
          out.writeLong(value.getPayload().getTimestamp());
          byte[] pl = value.getPayload().getPayload();
          if(pl == null)
            out.writeInt(-1);
          else
          {
            out.writeInt(pl.length);
            out.write(pl);
          }
          
        }
        out.writeInt(value.getSubmitCount());
        
      }

      @Override
      public QMessage deserialize(DataInput2 in, int available)
          throws IOException {
        QMessage qm = new QMessage();
        boolean haspayload = in.readBoolean();
        if(haspayload)
        {
          qm.setPayload(new Message());
          qm.getPayload().setCorrelationID(in.readUTF());
          qm.getPayload().setDestination(in.readUTF());
          qm.getPayload().setReplyTo(in.readUTF());
          qm.getPayload().setExpiryMillis(in.readLong());
          qm.getPayload().setTimestamp(in.readLong());
          int len = in.readInt();
          if(len > 0)
          {
            byte[] b = new byte[len];
            in.readFully(b);
            qm.getPayload().setPayload(b);
          }
          
        }
        
        if (haspayload) {
          qm.getPayload().setRedelivered(in.readInt() > 1);
        }
        
        return qm;

      }
    }).createOrOpen();
  }
  /**
   * 
   */
  public void start()
  {
    File f = new File(tempDBPath);
    if(!f.exists()){
      f.mkdirs();
    }
    try 
    {
      File dbFile = new File(f, qname+".db");
      if(!dbFile.exists())
        dbFile.createNewFile();
      
      //mapdb(dbFile);
      
    } catch (Exception e) {
      log.error("Temp persistence file not created ", e);
    }
    processUnacknowledged();
    running = true;
    threadPool.submit(this);
  }
  
  private void processUnacknowledged() {
    if(mapdb != null)
    {
     
      for(Entry<QID, QMessage> ent : mapdb.getEntries())
      {
        service.add(new MessageAndKey(ent.getKey(), ent.getValue()), true);
        log.info(":: Re-processed unacknowledged items");
      }
    }
    
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
        MessageAndKey m = service.poll(qname);
        log.debug("Polling done..");
        if (m.message != null) {
          if(mapdb != null)
          {
            mapdb.put(m.key, m.message);
          }
          threadPool.submit(new Worker(o, m));
        } 
      } finally {
        addObserver(o);
        threadPool.submit(this);
          
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
  private class Worker implements Runnable
  {

    public Worker(QueueListener o, MessageAndKey m) {
      super();
      this.o = o;
      this.m = m;
    }

    private final QueueListener o;
    private final MessageAndKey m;

    @Override
    public void run() {
      if (running) {
        notifyObserver(m, o);
      }
      else
      {
        service.add(m, false);
      }
    }
    
  }
  private void notifyObserver(final MessageAndKey m, final QueueListener o) 
  {
    try {
      o.update(null, m);
      if(mapdb != null)
      {
        mapdb.remove(m.key);
      }
    } catch (Exception e) {
      log.error("-- Container caught execption --", e);
      service.add(m, true);
    }
  
  }
  private String tempDBPath;
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
  public String getTempDBPath() {
    return tempDBPath;
  }
  public void setTempDBPath(String tempDBPath) {
    this.tempDBPath = tempDBPath;
  }

}
