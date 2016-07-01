/* ============================================================================
*
* FILE: SimpleQueueListener.java
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
package com.reactiva.hazelq;

import java.util.concurrent.ConcurrentSkipListSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.reactiva.hazelq.core.QueueListener;

public class SimpleQueueListener extends QueueListener {
  private static final Logger log = LoggerFactory.getLogger(SimpleQueueListener.class);
  private int iter = 1;
  private ConcurrentSkipListSet<Integer> set = new ConcurrentSkipListSet<>();
  
  static final String COUNTER_PREFIX = "payload=>";
  static final int MSG_COUNT = 100, MSG_OFFSET = 100;
  
  @Override
  protected void onMessage(Message m) throws Exception {
    
    /*if(m.getPayloadAsUTF().contains("63") && !m.isRedelivered())
      throw new Exception("Single exception for 63");*/
    String msg = m.getPayloadAsUTF();
    log.info("Got message:: "+msg);
    
    Integer n = Integer.valueOf(StringUtils.delete(msg, COUNTER_PREFIX));
    
    if(!set.isEmpty())
    {
      int last = set.last();
      try {
        Assert.isTrue(n >= last, "Last: "+last+" Next:"+n);
      } catch (Exception e) {
        log.error(e.getMessage());
      }
    }
    
    if(!set.add(n))
    {
      log.error("Duplicate: "+n);
    }
    if(n % MSG_OFFSET == 0)
      log.info("Consumed - "+(MSG_OFFSET*(iter++)));
    
    if(n == MSG_COUNT-1)
      log.info("* Consumed all messages *");
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      
    }
  }
}
