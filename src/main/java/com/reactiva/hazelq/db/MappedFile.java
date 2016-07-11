/* ============================================================================
*
* FILE: IndexedFile.java
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
package com.reactiva.hazelq.db;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
/**
 * 
 */
public class MappedFile implements Closeable, Runnable{

  private static final Logger log = LoggerFactory.getLogger(MappedFile.class);
  private RandomAccessFile dataFile, indexFile;
  private FileChannel dbChannel;
  private ReadWriteLock fileLock = new ReentrantReadWriteLock();
  private ScheduledExecutorService compactor;
  public static final int DEFAULT_CACHE_SIZE = 1000;
  /**
   * 
   * @param dir
   * @param fileName
   * @throws IOException
   */
  public MappedFile(String dir, String fileName) throws IOException
  {
    this(dir, fileName, false, DEFAULT_CACHE_SIZE);
  }
  /**
   * 
   * @param dir
   * @param fileName
   * @param usecompact
   * @throws IOException
   */
  public MappedFile(String dir, String fileName, boolean usecompact, int cacheSize) throws IOException
  {
    File f = new File(dir);
    if(!f.exists())
      f.mkdirs();
    
    File db = new File(f, fileName);
    if(!db.exists())
      db.createNewFile();
    
    File idx = new File(f, fileName+".idx");
    if(!idx.exists())
      idx.createNewFile();
    
    open(db, idx, cacheSize);
    
    if (usecompact) {
      compactor = Executors.newSingleThreadScheduledExecutor();
      compactor.scheduleWithFixedDelay(this, 60, 60, TimeUnit.SECONDS);
    }
  }
  /**
   * 
   * @param cacheSize 
   * @throws FileNotFoundException
   */
  private void open(File dbFile, File idxFile, int cacheSize) throws IOException 
  {
    
    dataFile = new RandomAccessFile(dbFile, "rwd");
    indexFile = new RandomAccessFile(idxFile, "rwd");
    
    dataFile.seek(dbFile.length());
    indexFile.seek(idxFile.length());
    
    dbChannel = dataFile.getChannel();
    
    dataMap = new WeakHashMap<>();
    indexMap = new LinkedHashMap<String, Offsets>((cacheSize+1), 1f, true){

      /**
       * 
       */
      private static final long serialVersionUID = 6521502326064918997L;
      
      @Override
      protected boolean removeEldestEntry(Map.Entry<String, Offsets> eldest) {
          return size() >= cacheSize;
      }
      
    };
  }
  
  private Long removeKey(String key) throws IOException
  {
    Offsets offsets = indexMap.get(key);
    markDeleted(offsets);
    indexMap.remove(key);
    dataMap.remove(offsets.data);
    return offsets.data;
  }
  private void markDeleted(Offsets idx) throws IOException
  {
    dataFile.seek(idx.data);
    dataFile.writeInt(-1);
    long ifptr = indexFile.getFilePointer();
    try
    {
      indexFile.seek(idx.idx);
      indexFile.writeBoolean(true);//deleted
    }
    finally
    {
      indexFile.seek(ifptr);
    }
  }
  /**
   * 
   * @param key
   * @return 
   * @throws IOException
   */
  public byte[] remove(String key) throws IOException
  {
    fileLock.writeLock().lock();
    long fp = dataFile.getFilePointer();
    try
    {
    
    	byte[] b = read0(key);
      if(b != null)
      {
        removeKey(key);
        return b;
      }
    }
    finally
    {
    	dataFile.seek(fp);
      fileLock.writeLock().unlock();
    }
    return null;
  }
  /**
   * Write the value bytes corresponding to the given key.
   * @param key
   * @param bytes
   * @return 
   * @throws IOException
   */
  public byte[] write(String key, byte[] bytes) throws IOException
  {
    byte[] prev = remove(key);//remove any existing key
    boolean err = false;
    fileLock.writeLock().lock();
    try 
    {
      long fp = dataFile.getFilePointer();
      dataFile.writeInt(bytes.length);
      dataFile.write(bytes);
      
      writeIndex(key, fp);
      return prev;
    } 
    catch(IOException ie)
    {
      err = true;
      throw ie;
    }
    finally {
      fileLock.writeLock().unlock();
      if(err && prev != null)
      {
        write(key, prev);
      }
    }
    
  }
  private Map<Long, byte[]> dataMap;
  private Map<String, Offsets> indexMap;
  /**
   * Get the record from given offset.
   * @param idx
   * @return
   * @throws IOException
   */
  private boolean readData(long idx) throws IOException
  {
    ByteBuffer buff = ByteBuffer.allocate(4);
    int read = dbChannel.read(buff, idx);
    Assert.isTrue(read == 4, "DB file corrupted");
    
    buff.flip();
    int len = buff.asIntBuffer().get();
    if(len == -1)
      return false; //deleted
    
    buff = ByteBuffer.allocate(len);
    
    long pos = idx + 4;
    read = dbChannel.read(buff, pos);
    while(buff.hasRemaining() && read > 0)
    {
      pos += read;
      read = dbChannel.read(buff, pos);
    }
    Assert.isTrue(read == len, "DB file corrupted");
    buff.flip();
    dataMap.put(idx, Arrays.copyOfRange(buff.array(), 0, read));
    return true;
  }
  /**
   * 
   * @param key
   * @return
   * @throws IOException
   */
  private byte[] read0(String key) throws IOException
  {

    if (!indexMap.containsKey(key)) {
      synchronized (indexMap) {
        if (!indexMap.containsKey(key)) {
          readIndex(key);
        }
      }
    }

    Offsets offsets = indexMap.get(key);
    if (offsets != null) {
      if (!dataMap.containsKey(offsets.data)) {
        boolean read = false;
        synchronized (dataMap) {
          if (!dataMap.containsKey(offsets.data)) {
            read = readData(offsets.data);
          }
        }
        if (!read)
          return null;
      }
      byte[] b = dataMap.get(offsets.data);
      if (b != null) {
        return Arrays.copyOf(b, b.length);
      }
    }
    return null;
  
  }
  /**
   * Read the value bytes for a given key, or returns null if none present.
   * @param key
   * @return
   * @throws IOException
   */
  public byte[] read(String key) throws IOException
  {
    fileLock.readLock().lock();
    try {
      return read0(key);
    } finally {
      fileLock.readLock().unlock();
    }

  }
  /**
   * 
   */
  private class IndexIterator implements Iterator<Long>,Closeable
  {

    private final long fptr;
    private long pos, idxOffset;
    
    long offset;
    int keyLen;
    private boolean deleted;
    byte [] b;

    public byte[] nextBytes()
    {
      return b;
    }
    public boolean isEmpty()
    {
      return fptr == 0;
    }
    /**
     * 
     * @throws IOException
     */
    public IndexIterator() throws IOException {
      fptr = indexFile.getFilePointer();
      pos = 0;
      indexFile.seek(pos);
    }
    @Override
    public boolean hasNext() {
      return pos < fptr;
    }

    private void fetch()
    {
      
      try 
      {
        idxOffset = pos;
        deleted = indexFile.readBoolean();
        pos += 1;
        keyLen = indexFile.readInt();
        pos += 4;
        b = new byte[keyLen];
        indexFile.read(b, 0, keyLen);
        pos += keyLen;
        offset = indexFile.readLong();
        pos += 8;
        
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }

    }
    @Override
    public Long next() {

      fetch();           
      return offset;
    }
    @Override
    public void close() throws IOException {
      indexFile.seek(fptr);    
    }
    
    public boolean isDeleted() {
      return deleted;
    }
    
    
  }
  /**
   * 
   */
  private static class Offsets
  {
    /**
     * 
     * @param data
     * @param idx
     */
    public Offsets(long data, long idx) {
      super();
      this.data = data;
      this.idx = idx;
    }

    private final long data, idx;
  }
  /**
   * Scan the index file for this key's offset.
   * @param key
   * @return 
   * @throws IOException
   */
  private boolean readIndex(String key) throws IOException
  {
    try(IndexIterator idxIter = new IndexIterator())
    {
      if(!idxIter.isEmpty())
      {
        Long offset;
        byte[] b;
        while(idxIter.hasNext())
        {
          offset = idxIter.next();
          if(idxIter.isDeleted())
            continue;
          b = idxIter.nextBytes();
          
          if(new String(b, StandardCharsets.UTF_8).equals(key))
          {
           indexMap.put(key, new Offsets(offset, idxIter.idxOffset)); 
           return true;
          }

          
        }
      }
    }
    return false;
  }

  private long writeIndex(String key, long fp) throws IOException {
    byte[] utf8 = key.getBytes(StandardCharsets.UTF_8);
    long ifp = indexFile.getFilePointer();
    
    indexFile.writeBoolean(false);//deleted
    indexFile.writeInt(utf8.length);
    indexFile.write(utf8);
    indexFile.writeLong(fp);
    log.debug("Index offsets written "+ifp+":"+indexFile.getFilePointer());
    return ifp;
  }

  @Override
  public void close() throws IOException {
    if (compactor != null) {
      compactor.shutdown();
      try {
        compactor.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
      } 
    }
    indexFile.close();
    dataFile.close();
    dbChannel.close();
  }
  @Override
  public void run() {
    compact();
  }
  private void compact() {
    fileLock.writeLock().lock();
    try
    {
      //TODO: compactor. index files have a boolean true
    }
    finally
    {
      fileLock.writeLock().unlock();
    }
    
  }
  
}
