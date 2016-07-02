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
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.springframework.util.Assert;

class IndexedFile implements Closeable, Runnable{

  
  private RandomAccessFile dbaseFile, indexFile;
  private FileChannel dbChannel;
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
    
    dbaseFile = new RandomAccessFile(dbFile, "rwd");
    indexFile = new RandomAccessFile(idxFile, "rwd");
    dbaseFile.seek(dbFile.length());
    indexFile.seek(idxFile.length());
    
    dbChannel = dbaseFile.getChannel();
  }
  /**
   * 
   * @param key
   * @throws IOException
   */
  public void remove(String key) throws IOException
  {
    fileLock.writeLock().lock();
    long fp = dbaseFile.getFilePointer();
    try
    {
    
    	byte[] b = read(key);
      if(b != null)
      {
        Long idx = indexMap.get(key);
        dbaseFile.seek(idx);
        dbaseFile.writeInt(-1);
        indexMap.remove(key);
        dataMap.remove(idx);
      }
    }
    finally
    {
    	dbaseFile.seek(fp);
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
      long fp = dbaseFile.getFilePointer();
      dbaseFile.writeInt(bytes.length);
      dbaseFile.write(bytes);
      
      writeIndex(key, fp);
    } 
    finally {
      fileLock.writeLock().unlock();
    }
    
  }
  private Map<Long, byte[]> dataMap = new WeakHashMap<>();
  private Map<String, Long> indexMap = new HashMap<>();
  /**
   * Get the record from given offset.
   * @param idx
   * @return
   * @throws IOException
   */
  private boolean read(long idx) throws IOException
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
  public byte[] read(String key) throws IOException
  {
    fileLock.readLock().lock();
    try
    {
      if(!indexMap.containsKey(key)){
        synchronized (indexMap) {
			if (!indexMap.containsKey(key)) {
				read0(key);
			}
		}
      }
      
      Long idx = indexMap.get(key);
      if(idx != null)
      {
        if(!dataMap.containsKey(idx))
        {
          boolean read = false;
		synchronized (dataMap) {
			if (!dataMap.containsKey(idx)) {
				read = read(idx);
			}
		}
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
  /**
   * Scan the index file for this key's offset.
   * @param key
   * @throws IOException
   */
  private void read0(String key) throws IOException
  {
    long fp = indexFile.getFilePointer();
    try 
    {
      if(fp == 0)
    	  return;
      
      long pos = 0;
      indexFile.seek(pos);
      
      int keyLen;
      byte [] b;
      long offset;
      while (pos < fp) 
      {
    	 keyLen = indexFile.readInt();
    	 pos += 4;
    	 b = new byte[keyLen];
    	 indexFile.read(b, 0, keyLen);
    	 pos += keyLen;
    	 offset = indexFile.readLong();
    	 pos += 8;
    	 if(new String(b, StandardCharsets.UTF_8).equals(key))
    	 {
    		indexMap.put(key, offset); 
    		return;
    	 }
      }
           
    } 
    finally {
      indexFile.seek(fp);
    }
    
  }

  private void writeIndex(String key, long fp) throws IOException {
    byte[] utf8 = key.getBytes(StandardCharsets.UTF_8);
    indexFile.writeInt(utf8.length);
    indexFile.write(utf8);
    indexFile.writeLong(fp);
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
    dbaseFile.close();
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
      
    }
    finally
    {
      fileLock.writeLock().unlock();
    }
    
  }
  
  private static void test(IndexedFile ifile)
  {

		try {
			byte[] b = ifile.read("two");
			  System.out.println(Thread.currentThread().getName() + " => " + new String(b, StandardCharsets.UTF_8));
			  
			  /*b = ifile.read("3");
			  System.out.println(Thread.currentThread().getName() + " => " + new String(b, StandardCharsets.UTF_8));*/
			  
			  b = ifile.read("4");
			  System.out.println(Thread.currentThread().getName() + " => 4" + b);
			  
			  ifile.remove("3");
			  
			  
			  b = ifile.read("3");
			  System.out.println(Thread.currentThread().getName() + " => 3 deleted "+b);

			  b = ifile.read("two");
			  System.out.println(Thread.currentThread().getName() + " => " + new String(b, StandardCharsets.UTF_8));
			  
			  b = ifile.read("one");
			  System.out.println(Thread.currentThread().getName() + " => " + new String(b, StandardCharsets.UTF_8));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	
  }
  public static void main(String[] args) throws IOException {
    final IndexedFile ifile = new IndexedFile("C:\\workspace\\data\\hazelq", "test.db");
    try
    {
      /*ifile.write("one", "one".getBytes(StandardCharsets.UTF_8));
      ifile.write("two", "two".getBytes(StandardCharsets.UTF_8));
      ifile.write("3", "3".getBytes(StandardCharsets.UTF_8));*/
    	
    	//test(ifile);
      
      ExecutorService t = Executors.newFixedThreadPool(2);
            
      t.submit(new Runnable() {
  		
  		@Override
  		public void run() {
  			test(ifile);
  			
  		}
  	});
      
      t.submit(new Runnable() {
  		
  		@Override
  		public void run() {
  			test(ifile);
  		}
  	});
      
      t.submit(new Runnable() {
    		
    		@Override
    		public void run() {
    			test(ifile);
    		}
    	});
      
      t.shutdown();
      try {
		t.awaitTermination(10, TimeUnit.MINUTES);
	} catch (InterruptedException e) {
		
	}
      
    }
    finally
    {
      ifile.close();
    }
  }
}
