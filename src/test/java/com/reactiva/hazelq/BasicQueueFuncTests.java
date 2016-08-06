/* ============================================================================
*
* FILE: BasicQueueFuncTests.java
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

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.reactiva.hazelq.core.QueueService;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = {HQServer.class})
public class BasicQueueFuncTests {

  @Autowired
  QueueService service;

  
  private static final Logger log = LoggerFactory.getLogger(BasicQueueFuncTests.class);
  private void pub()
  {

    new Thread(){
      public void run()
      {
        String s;
        for(int i=0; i<SimpleQueueListener.MSG_COUNT; i++)
        {
          s = SimpleQueueListener.COUNTER_PREFIX+ai.incrementAndGet();
          log.debug("Submitting: "+s);
          service.add(new Message(s, "testQ"));
        }
        
        log.info("######### Submitted test messages ##########");
      }
    }.start();
    
    

  
  }
  AtomicInteger ai = new AtomicInteger();
}
