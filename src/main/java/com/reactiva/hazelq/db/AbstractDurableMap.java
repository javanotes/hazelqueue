/* ============================================================================
*
* FILE: AbstractDurableMap.java
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
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public abstract class AbstractDurableMap<K,V> implements Map<K, V>, Closeable {

  private final MappedFile file;
  /**
   * 
   * @param dir
   * @param fileName
   */
  public AbstractDurableMap(String dir, String fileName)
  {
    try {
      file = new MappedFile(dir, fileName);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }
  /**
   * Generic serialization scheme for key.
   * @param key
   * @return
   */
  protected abstract String keyToString(K key);
  /**
   * Generic de-serialization scheme for value.
   * @param val
   * @return
   */
  protected abstract V bytesToValue(byte[] val);
  /**
   * Generic de-serialization scheme for value.
   * @param val
   * @return
   */
  protected abstract byte[] valueToBytes(V val);
  @Override
  public int size() {
    try {
      return file.size();
    } catch (IOException e) {
      throw new MapIOException(e);
    }
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean containsKey(Object key) {
    try {
      return file.contains(keyToString((K) key));
    } catch (IOException e) {
      throw new MapIOException(e);
    }
  }

  @Override
  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException(); 
  }

  @SuppressWarnings("unchecked")
  @Override
  public V get(Object key) {
    byte[] bytes;
    try {
      bytes = file.read(keyToString((K) key));
    } catch (IOException e) {
      throw new MapIOException(e);
    }
    return bytesToValue(bytes);
  }

  @Override
  public V put(K key, V value) {
    byte[] val;
    try {
      val = file.write(keyToString(key), valueToBytes(value));
    } catch (IOException e) {
      throw new MapIOException(e);
    }
    return bytesToValue(val);
  }

  @SuppressWarnings("unchecked")
  @Override
  public V remove(Object key) {
    byte[] val;
    try {
      val = file.remove(keyToString((K) key));
    } catch (IOException e) {
      throw new MapIOException(e);
    }
    return bytesToValue(val);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    for(java.util.Map.Entry<? extends K, ? extends V> e : m.entrySet())
    {
      put(e.getKey(), e.getValue());
    }
    
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<K> keySet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<V> values() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<java.util.Map.Entry<K, V>> entrySet() {
    throw new UnsupportedOperationException();
  }
  @Override
  public void close() throws IOException {
    file.close();
    
  }

}
