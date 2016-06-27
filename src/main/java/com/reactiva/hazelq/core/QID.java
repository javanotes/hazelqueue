/* ============================================================================
*
* FILE: QID.java
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

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.reactiva.hazelq.utils.TimeUIDComparator;
import com.reactiva.hazelq.utils.TimeUIDGen;

public class QID implements DataSerializable, Serializable, Comparable<QID> {

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((uid == null) ? 0 : uid.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    QID other = (QID) obj;
    if (uid == null) {
      if (other.uid != null)
        return false;
    } else if (this.compareTo(other) != 0)
      return false;
    
    return true;
  }
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  public QID(UUID uid) {
    super();
    this.setUid(uid);
  }

  public QID()
  {
    
  }
  private UUID uid;
  @Override
  public void writeData(ObjectDataOutput out) throws IOException {
    out.writeByteArray(TimeUIDGen.decompose(uid));
    
  }

  @Override
  public void readData(ObjectDataInput in) throws IOException {
    byte[] bytes = in.readByteArray();
    setUid(TimeUIDGen.getUUID(ByteBuffer.wrap(bytes)));
    
  }

  public UUID getUid() {
    return uid;
  }

  public void setUid(UUID uid) {
    this.uid = uid;
  }

  @Override
  public int compareTo(QID other) {
    return new TimeUIDComparator().compare(this.uid, other.uid);
  }

  @Override
  public String toString() {
    return "QMessageKey [uid=" + uid + "]";
  }


}
