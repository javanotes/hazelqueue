/* ============================================================================
*
* FILE: AbstractLocalPutMapEntryCallback.java
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
package com.reactiva.hazelq.grid;

import java.io.Serializable;

import com.hazelcast.core.HazelcastInstance;

public abstract class AbstractLocalMapEntryListener<V>
    implements LocalMapEntryListener<V> {

  protected HazelcastInstance hzService;
  protected String listenerId;
  protected boolean registerSelf;
  protected void register()
  {
    if (registerSelf) {
      listenerId = hzService.getMap(keyspace()).addLocalEntryListener(this);
    }
  }
  /**
   * 
   * @param hzService
   */
  public AbstractLocalMapEntryListener(HazelcastInstance hzService) {
    this(hzService, true);
  }
  /**
   * 
   * @param hzService
   * @param registerSelf
   */
  public AbstractLocalMapEntryListener(HazelcastInstance hzService, boolean registerSelf) {
    this.hzService = hzService;
    this.registerSelf = registerSelf;
  }
  /**
   * De-register the local map entry listener.
   */
  protected void removeMapListener()
  {
    if (listenerId != null) {
      hzService.getMap(keyspace()).removeEntryListener(listenerId);
    }
  }
  /**
   * Sets an item to the IMap on which this listener is registered.
   * @param key
   * @param value
   */
  public void putEntry(Serializable key, V value)
  {
    putEntry(key, value, true);
  }
  /**
   * 
   * @param key
   * @param value
   * @param set
   */
  public void putEntry(Serializable key, V value, boolean set)
  {
    if(set)
      hzService.getMap(keyspace()).set(key, value);
    else
      hzService.getMap(keyspace()).put(key, value);
  }

}
