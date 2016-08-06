/* ============================================================================
*
* FILE: QueueListener.java
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

import java.util.Observable;

import com.reactiva.hazelq.Message;
/**
 * Abstract base class to be extended for registering queue listeners.
 * @see IQueueService#registerListener(QueueListener, String)
 */
public abstract class QueueListener  {

  @Override
  public final int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((identifier() == null) ? 0 : identifier().hashCode());
    return result;
  }
  @Override
  public final boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    QueueListener other = (QueueListener) obj;
    if (identifier() == null) {
      if (other.identifier() != null)
        return false;
    } else if (!identifier().equals(other.identifier()))
      return false;
    return true;
  }

  /**
   * To be overridden to provide a listener identifier.
   * @return
   */
  public String identifier()
  {
    return hashCode()+"";
  }
  /**
   * To be overridden to increase concurrency.
   * @return
   */
  public int concurrency()
  {
    return 1;
  }
  /**
   * Callback method invoked on message added to queue.
   * @param m
   * @throws Exception 
   */
  protected abstract void onMessage(Message m) throws Exception;
  /**
   * Used internally.
   * @param arg0
   * @param arg1
   * @throws Exception
   */
  final void update(Observable arg0, Object arg1) throws Exception {
    MessageAndKey mk = (MessageAndKey) arg1;
    onMessage(mk.message.getPayload());
  }

}
