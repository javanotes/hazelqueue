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
package com.reactiva.hazelq;

import java.nio.charset.StandardCharsets;
/**
 * 
 */
public class Message {

  /**
   * Default constructor.
   */
  public Message(){}
  /**
   * New message with a UTF8 encoded payload.
   * @param s
   */
  public Message(String payload) {
    setPayload(payload.getBytes(StandardCharsets.UTF_8));
  }
  /**
   * New message with a UTF8 encoded payload and destination queue.
   * @param payload
   * @param destination
   */
  public Message(String payload, String destination) {
    this(payload);
    setDestination(destination);
  }
  /**
   * New message with a UTF8 encoded payload, destination queue and a correlation ID.
   * @param payload
   * @param destination
   * @param corrID
   */
  public Message(String payload, String destination, String corrID) {
    this(payload, destination);
    setCorrelationID(corrID);
  }
  private byte[] payload;
  public String getCorrelationID() {
    return correlationID;
  }
  public void setCorrelationID(String correlationID) {
    this.correlationID = correlationID;
  }
  public long getTimestamp() {
    return timestamp;
  }
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }
  public String getDestination() {
    return destination;
  }
  public void setDestination(String destination) {
    this.destination = destination;
  }
  public String getReplyTo() {
    return replyTo;
  }
  public void setReplyTo(String replyTo) {
    this.replyTo = replyTo;
  }
  public boolean isRedelivered() {
    return redelivered;
  }
  public void setRedelivered(boolean redelivered) {
    this.redelivered = redelivered;
  }
  public long getExpiryMillis() {
    return expiryMillis;
  }
  public void setExpiryMillis(long expiryMillis) {
    this.expiryMillis = expiryMillis;
  }
  public byte[] getPayload() {
    return payload;
  }
  public String getPayloadAsUTF() {
    return new String(getPayload(), StandardCharsets.UTF_8);
  }
  public void setPayload(byte[] payload) {
    this.payload = payload;
  }
  String correlationID;
  long timestamp;
  String destination, replyTo;
  boolean redelivered;
  long expiryMillis;
}
