/* ============================================================================
*
* FILE: MappedFile.java
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.WeakHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.reactiva.hazelq.utils.DirectMem;
/**
 * @Experimental
 * A local disk persistence system for storing key value records. The key is to be a UTF8 encoded string, and
 * values are serialized bytes. Can be used as a simple file backed map data structure.
 * <p><b>Note:</b> The compaction of deleted records is in WIP state.
 */
class MappedFile implements Closeable, Runnable{

  private static final Logger log = LoggerFactory.getLogger(MappedFile.class);
  private RandomAccessFile dataFile;
  private RandomAccessFile indexFile;
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
  private final String fileName;
  /**
   * 
   * @param dir
   * @param fileName
   * @param usecompact
   * @throws IOException
   */
  public MappedFile(String dir, String fileName, boolean usecompact, int cacheSize) throws IOException
  {
    this.fileName = fileName;
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
    setIndexFile(new RandomAccessFile(idxFile, "rwd"));
    
    dataFile.seek(dbFile.length());
    getIndexFile().seek(idxFile.length());
    
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
    long ifptr = getIndexFile().getFilePointer();
    try
    {
      getIndexFile().seek(idx.idx);
      getIndexFile().writeBoolean(true);//deleted
    }
    finally
    {
      getIndexFile().seek(ifptr);
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
    Assert.notNull(key, "Null key not supported");
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
   * Write the value bytes corresponding to the given key. Null key/value not supported.
   * @param key
   * @param bytes
   * @return 
   * @throws IOException
   */
  public byte[] write(String key, byte[] bytes) throws IOException
  {
    Assert.notNull(key, "Null key not supported");
    Assert.notNull(bytes, "Null value not supported");
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
    Assert.notNull(key, "Null key not supported");
    fileLock.readLock().lock();
    try {
      return read0(key);
    } finally {
      fileLock.readLock().unlock();
    }

  }
  /**
   * Scan the index file for this key's offset.
   * @param key
   * @return 
   * @throws IOException
   */
  private boolean readIndex(String key) throws IOException
  {
    try(IndexIterator idxIter = new IndexIterator(this))
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
           indexMap.put(key, new Offsets(offset, idxIter.getIdxOffset())); 
           return true;
          }

          
        }
      }
    }
    return false;
  }

  private long writeIndex(String key, long fp) throws IOException {
    byte[] utf8 = key.getBytes(StandardCharsets.UTF_8);
    long ifp = getIndexFile().getFilePointer();
    
    getIndexFile().writeBoolean(false);//deleted
    getIndexFile().writeInt(utf8.length);
    getIndexFile().write(utf8);
    getIndexFile().writeLong(fp);
    log.debug("Index offsets written "+ifp+":"+getIndexFile().getFilePointer());
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
    getIndexFile().close();
    dataFile.close();
    dbChannel.close();
  }
  @Override
  public void run() {
    try {
      compact();
    } catch (IOException e) {
      log.error("-- Compaction task error --", e);
    }
  }
  /**
   * Scans the index file to check if the key is present. Null key not supported
   * @param key
   * @return
   * @throws IOException
   */
  public boolean contains(String key) throws IOException
  {
    Assert.notNull(key, "Null key not supported");
    MappedByteBuffer buff = null;
    fileLock.readLock().lock();
    try
    {
      long len = getIndexFile().length();
      buff = getIndexFile().getChannel().map(MapMode.READ_ONLY, 0, len);
      byte[] b;
      while(buff.hasRemaining())
      {
        boolean deleted = buff.get() != 0;
        
        int b_len = buff.getInt();
        b = new byte[b_len];
        buff.get(b);
        buff.getLong();
        
        if(!deleted)
        {
          if(key.equals(new String(b, StandardCharsets.UTF_8)))
          {
            return true;
          }
        }
      }
      
    }
    finally
    {
      if (buff != null) {
        buff.clear();
        DirectMem.unmap(buff);
      }
      fileLock.readLock().unlock();
    }
    return false;
    
  
  }
  /**
   * Scans the index file to get the size of the map.
   * @return
   * @throws IOException
   */
  public int size() throws IOException
  {
    MappedByteBuffer buff = null;
    fileLock.readLock().lock();
    try
    {
      long len = getIndexFile().length();
      buff = getIndexFile().getChannel().map(MapMode.READ_ONLY, 0, len);
      int count = 0;
      while(buff.hasRemaining())
      {
        boolean deleted = buff.get() != 0;
        if(!deleted)
          count++;
        int b_len = buff.getInt();
        buff.get(new byte[b_len]);
        buff.getLong();
      }
      
      return count;
    }
    finally
    {
      if (buff != null) {
        buff.clear();
        DirectMem.unmap(buff);
      }
      fileLock.readLock().unlock();
    }
    
  }
  
  /**
   * @WIP
   * Compact the files by removing fragmentation caused by deleted records.
   * This is an expensive operation and should be used with discretion.
   * @throws IOException 
   */
  public void compact() throws IOException {
    fileLock.writeLock().lock();
    try
    {
      //TODO: compactor. index files have a boolean true

      Set<Long> delIdxList = new TreeSet<>();
      Set<Long> delDatList = new TreeSet<>();
      
      try(IndexIterator idxIter = new IndexIterator(this))
      {
        if(!idxIter.isEmpty())
        {
          Long offset;
          while(idxIter.hasNext())
          {
            offset = idxIter.next();
            if(idxIter.isDeleted())
            {
              delDatList.add(offset);
              delIdxList.add(idxIter.getIdxOffset());
            }
            
                       
          }
        }
      } catch (IOException e) {
        throw e;
      }
      
      deleteIndices(delIdxList);
      deleteData(delDatList);
      clearCache();
    }
    finally
    {
      fileLock.writeLock().unlock();
    }
    
  }
  private void clearCache() {
    dataMap.clear();
    indexMap.clear();
  }
  private void deleteIndices(Set<Long> offsets) throws IOException 
  {
    String tmpFileName = fileName+"_i_"+System.currentTimeMillis();
    WritableByteChannel bkpFile = Channels.newChannel(new FileOutputStream(File.createTempFile(tmpFileName, null)));
    getIndexFile().getChannel().transferTo(0, getIndexFile().length(), bkpFile);
    
    
    
    long pos = 0;
    getIndexFile().seek(pos);
    int len = 13;
    for(Long off : offsets)
    {
      pos += off;
      getIndexFile().seek(pos);
      
      //1 + 4 + array.len + 8
      getIndexFile().readBoolean();
      len += getIndexFile().readInt();
      
      getIndexFile().seek(pos);//go back
      long nextPos = pos + len;
      
      getIndexFile().getChannel().transferTo(pos, getIndexFile().length(), bkpFile);
      
      len = 13;
    }
  }
  private void deleteData(Set<Long> offsets) throws FileNotFoundException, IOException {
    String tmpFileName = fileName+"_d_"+System.currentTimeMillis();
    WritableByteChannel bkpFile = Channels.newChannel(new FileOutputStream(File.createTempFile(tmpFileName, null)));
    dataFile.getChannel().transferTo(0, dataFile.length(), bkpFile);
  }
  
  public RandomAccessFile getIndexFile() {
    return indexFile;
  }
  public void setIndexFile(RandomAccessFile indexFile) {
    this.indexFile = indexFile;
  }

  
}
