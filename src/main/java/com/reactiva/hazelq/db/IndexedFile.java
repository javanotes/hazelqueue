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
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.springframework.util.Assert;

public class IndexedFile implements Closeable, Runnable{

  
  private RandomAccessFile dbChn, idxChn;
  private ReadWriteLock fileLock = new ReentrantReadWriteLock();
  private ScheduledExecutorService compactor;
  /**
   * 
   * @param dir
   * @param fileName
   * @throws IOException
   */
  public IndexedFile(String dir, String fileName) throws IOException
  {
    this(dir, fileName, false);
  }
  /**
   * 
   * @param dir
   * @param fileName
   * @param usecompact
   * @throws IOException
   */
  public IndexedFile(String dir, String fileName, boolean usecompact) throws IOException
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
    
    open(db, idx);
    
    if (usecompact) {
      compactor = Executors.newSingleThreadScheduledExecutor();
      compactor.scheduleWithFixedDelay(this, 60, 60, TimeUnit.SECONDS);
    }
  }
  /**
   * 
   * @throws FileNotFoundException
   */
  private void open(File dbFile, File idxFile) throws IOException 
  {
    
    dbChn = new RandomAccessFile(dbFile, "rwd");
    idxChn = new RandomAccessFile(idxFile, "rwd");
    dbChn.seek(dbChn.length());
    idxChn.seek(idxChn.length());
  }
  /**
   * 
   * @param key
   * @throws IOException
   */
  public void remove(String key) throws IOException
  {
    fileLock.writeLock().lock();
    try
    {
      if(indexMap.containsKey(key))
      {
        Long idx = indexMap.get(key);
        ByteBuffer buff = ByteBuffer.allocate(4);
        buff.asIntBuffer().put(0);
        buff.flip();
        dbChn.getChannel().write(buff, idx);
        indexMap.remove(key);
        dataMap.remove(idx);
      }
    }
    finally
    {
      fileLock.writeLock().unlock();
    }
  }
  /**
   * 
   * @param key
   * @param bytes
   * @throws IOException
   */
  public void write(String key, byte[] bytes) throws IOException
  {
    fileLock.writeLock().lock();
    try 
    {
      long fp = dbChn.getFilePointer();
      dbChn.writeInt(bytes.length);
      dbChn.write(bytes);
      
      writeIndex(key, fp);
    } 
    finally {
      fileLock.writeLock().unlock();
    }
    
  }
  private Map<Long, byte[]> dataMap = new WeakHashMap<>();
  private Map<String, Long> indexMap = new HashMap<>();
  
  private boolean read0(long idx) throws IOException
  {
    ByteBuffer buff = ByteBuffer.allocate(4);
    int read = dbChn.getChannel().read(buff, idx);
    Assert.isTrue(read == 4, "DB file corrupted");
    
    buff.flip();
    int len = buff.asIntBuffer().get();
    if(len == 0)
      return false; //deleted
    
    buff = ByteBuffer.allocate(len);
    
    long pos = idx + 4;
    read = dbChn.getChannel().read(buff, pos);
    while(buff.hasRemaining() && read != -1)
    {
      pos += read;
      read = dbChn.getChannel().read(buff, pos);
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
  public byte[] read(String key) throws IOException
  {
    fileLock.readLock().lock();
    try
    {
      if(!indexMap.containsKey(key))
        readIdx(key);
      
      Long idx = indexMap.get(key);
      if(idx != null)
      {
        if(!dataMap.containsKey(idx))
        {
          boolean read = read0(idx);
          if(!read)
            return null;
        }
        byte[] b = dataMap.get(idx);
        return Arrays.copyOf(b, b.length);
      }
    }
    finally
    {
      fileLock.readLock().unlock();
    }
    return null;
  }
  private void readIdx(String key) throws IOException
  {
    
    try 
    {
      long pos = 0;
      ByteBuffer dst;
      int var = 0;
      byte[] b;
      
      long offset = pos;
      int totalRead = 0;
      int read = 0;
      
      long len = idxChn.length();
      FileChannel channel = idxChn.getChannel();
    
      while (pos < len) 
      {
        
            dst = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
            
            totalRead = 0;
            read = 0;
            do
            {
              read = channel.read(dst, offset);
              offset += read;
              totalRead += read;
            }
            while(dst.hasRemaining() && read > 0);
            
            if(totalRead == -1)
              break;//nothing has been written yet
            Assert.isTrue(totalRead == 4, "Index file corrupted. keystring len not match "+totalRead);
            dst.flip();
            pos += totalRead;
            
            var = dst.asIntBuffer().get();
            dst = ByteBuffer.allocate(var).order(ByteOrder.BIG_ENDIAN);
            
            offset = pos;
            totalRead = 0;
            read = 0;
            do
            {
              read = channel.read(dst, offset);
              offset += read;
              totalRead += read;
            }
            while(dst.hasRemaining() && read > 0);
            
            Assert.isTrue(var == totalRead, "Index file corrupted. Expected len="+var+" got read="+totalRead);
            
            dst.flip();
            pos += totalRead;
            
            b = Arrays.copyOfRange(dst.array(), 0, totalRead);
            
            if (new String(b, StandardCharsets.UTF_8).equals(key)) 
            {
              dst = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
              Assert.isTrue(8 == channel.read(dst, pos), "Index file corrupted");
              dst.flip();
              long index = dst.asLongBuffer().get();
              indexMap.put(key, index);
              break;
            } 
          
      }
      
      
      
    } 
    finally {
      
    }
    
  }

  private void writeIndex(String key, long fp) throws IOException {
    byte[] utf8 = key.getBytes(StandardCharsets.UTF_8);
    idxChn.writeInt(utf8.length);
    idxChn.write(utf8);
    idxChn.writeLong(fp);
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
    idxChn.close();
    dbChn.close();
  }
  @Override
  public void run() {
    compact();
  }
  private void compact() {
    fileLock.writeLock().lock();
    try
    {
      
    }
    finally
    {
      fileLock.writeLock().unlock();
    }
    
  }
  
  public static void main(String[] args) throws IOException {
    IndexedFile ifile = new IndexedFile("C:\\data\\hazelq", "test.db");
    try
    {
      ifile.write("one", "one".getBytes(StandardCharsets.UTF_8));
      ifile.write("two", "two".getBytes(StandardCharsets.UTF_8));
      ifile.write("3", "3".getBytes(StandardCharsets.UTF_8));
      
      byte[] b = ifile.read("two");
      System.out.println(new String(b, StandardCharsets.UTF_8));
      
      b = ifile.read("3");
      System.out.println(new String(b, StandardCharsets.UTF_8));
      
      b = ifile.read("4");
      System.out.println("4" + b);
      
      ifile.remove("3");
      
      
      b = ifile.read("3");
      System.out.println("3" + b);

    }
    finally
    {
      ifile.close();
    }
  }
}
