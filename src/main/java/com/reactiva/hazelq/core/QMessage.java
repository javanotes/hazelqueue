/* ============================================================================
*
* FILE: Message.java
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
import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.reactiva.hazelq.Message;

class QMessage implements DataSerializable {

  public QMessage() {
    
  }
  public QMessage(Message payload) {
    super();
    this.payload = payload;
  }
  public Message getPayload() {
    return payload;
  }
  
  private AtomicInteger submit = new AtomicInteger(1);

  public int getSubmitCount() {
    return submit.get();
  }
    public void setPayload(Message payload) {
    this.payload = payload;
  }
  private Message payload;
  @Override
  public void writeData(ObjectDataOutput out) throws IOException {
    if(payload == null)
    {
      out.writeBoolean(false);
    }
    else
    {
      out.writeBoolean(true);
      out.writeUTF(payload.getCorrelationID());
      out.writeUTF(payload.getDestination());
      out.writeUTF(payload.getReplyTo());
      out.writeLong(payload.getExpiryMillis());
      out.writeLong(payload.getTimestamp());
      out.writeByteArray(payload.getPayload());
    }
    out.writeInt(submit.get());
  }

  @Override
  public void readData(ObjectDataInput in) throws IOException {
    boolean haspayload = in.readBoolean();
    if(haspayload)
    {
      payload = new Message();
      payload.setCorrelationID(in.readUTF());
      payload.setDestination(in.readUTF());
      payload.setReplyTo(in.readUTF());
      payload.setExpiryMillis(in.readLong());
      payload.setTimestamp(in.readLong());
      payload.setPayload(in.readByteArray());
    }
    submit = new AtomicInteger(in.readInt());
    
    if (haspayload) {
      payload.setRedelivered(submit.get() > 1);
    }
  }
  void incrRedelivery()
  {
    submit.incrementAndGet();
  }
}
