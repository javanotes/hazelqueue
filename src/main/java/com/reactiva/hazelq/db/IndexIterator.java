/* ============================================================================
*
* FILE: IndexIterator.java
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
import java.io.IOException;
import java.util.Iterator;

/**
 * 
 */
class IndexIterator implements Iterator<Long>,Closeable
{

  /**
   * 
   */
  private final MappedFile mappedFile;
  private final long fptr;
  private long pos;
  private long idxOffset;
  
  private long offset;
  private int keyLen;
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
   * @param mappedFile TODO
   * @throws IOException
   */
  public IndexIterator(MappedFile mappedFile) throws IOException {
    this.mappedFile = mappedFile;
    fptr = this.mappedFile.getIndexFile().getFilePointer();
    pos = 0;
    this.mappedFile.getIndexFile().seek(pos);
  }
  @Override
  public boolean hasNext() {
    return pos < fptr;
  }

  private void fetch()
  {
    
    try 
    {
      setIdxOffset(pos);
      deleted = this.mappedFile.getIndexFile().readBoolean();
      pos += 1;
      keyLen = this.mappedFile.getIndexFile().readInt();
      pos += 4;
      b = new byte[keyLen];
      this.mappedFile.getIndexFile().read(b, 0, keyLen);
      pos += keyLen;
      offset = this.mappedFile.getIndexFile().readLong();
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
    this.mappedFile.getIndexFile().seek(fptr);    
  }
  
  public boolean isDeleted() {
    return deleted;
  }
  public long getIdxOffset() {
    return idxOffset;
  }
  private void setIdxOffset(long idxOffset) {
    this.idxOffset = idxOffset;
  }
  
  
}