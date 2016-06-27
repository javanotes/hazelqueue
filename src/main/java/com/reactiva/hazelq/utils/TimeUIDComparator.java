/* ============================================================================
*
* FILE: TimeUIDComparator.java
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
package com.reactiva.hazelq.utils;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.UUID;

/**
 * 
 */
public class TimeUIDComparator implements Comparator<UUID>
{

  private int compareCustom(ByteBuffer b1, ByteBuffer b2)
  {
      // Compare for length
      int s1 = b1.position(), s2 = b2.position();
      int l1 = b1.limit(), l2 = b2.limit();

      // should we assert exactly 16 bytes (or 0)? seems prudent
      boolean p1 = l1 - s1 == 16, p2 = l2 - s2 == 16;
      if (!(p1 & p2))
      {
          assert p1 | (l1 == s1);
          assert p2 | (l2 == s2);
          return p1 ? 1 : p2 ? -1 : 0;
      }

      long msb1 = b1.getLong(s1);
      long msb2 = b2.getLong(s2);
      msb1 = reorderTimestampBytes(msb1);
      msb2 = reorderTimestampBytes(msb2);

      assert (msb1 & topbyte(0xf0L)) == topbyte(0x10L);
      assert (msb2 & topbyte(0xf0L)) == topbyte(0x10L);

      int c = Long.compare(msb1, msb2);
      if (c != 0)
          return c;

      // this has to be a signed per-byte comparison for compatibility
      // so we transform the bytes so that a simple long comparison is equivalent
      long lsb1 = signedBytesToNativeLong(b1.getLong(s1 + 8));
      long lsb2 = signedBytesToNativeLong(b2.getLong(s2 + 8));
      return Long.compare(lsb1, lsb2);
  }
  
  private static long topbyte(long topbyte)
  {
      return topbyte << 56;
  }

  protected static long reorderTimestampBytes(long input)
  {
      return    (input <<  48)
                | ((input <<  16) & 0xFFFF00000000L)
                |  (input >>> 32);
  }

  // takes as input 8 signed bytes in native machine order
  // returns the first byte unchanged, and the following 7 bytes converted to an unsigned representation
  // which is the same as a 2's complement long in native format
  private static long signedBytesToNativeLong(long signedBytes)
  {
      return signedBytes ^ 0x0080808080808080L;
  }
  @Override
  public int compare(UUID u1, UUID u2) {
    return compareCustom(ByteBuffer.wrap(TimeUIDGen.decompose(u1)), ByteBuffer.wrap(TimeUIDGen.decompose(u2)));
  }
  
}