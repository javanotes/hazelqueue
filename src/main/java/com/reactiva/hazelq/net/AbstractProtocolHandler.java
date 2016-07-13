/* ============================================================================
*
* FILE: AbstractProtocolHandler.java
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
package com.reactiva.hazelq.net;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * An abstract implementation of a {@linkplain ProtocolHandler}. Applications
 * should need to override {@link #doProcess(DataInputStream)} and
 * {@link #doRead(SocketChannel)} methods.
 * 
 * @see ProtocolHandlerFactory
 */
public abstract class AbstractProtocolHandler implements ProtocolHandler {
	/**
	 * 
	 */
	protected final ByteArrayOutputStream writeStream;
	/**
	 * 
	 */
	protected final ByteBuffer readBuffer;
	protected long totalRead = 0;
	protected int read = 0;

	/**
	 * 
	 */
	public AbstractProtocolHandler() {
		readBuffer = ByteBuffer.allocate(ServerSocketListener.DEFAULT_READ_BUFF_SIZE);
		writeStream = new ByteArrayOutputStream();
	}

	@Override
	public void close() {
		readBuffer.clear();
		writeStream.reset();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.reactivetechnologies.csvloader.server.ProtocolHandler#doProcess(java.
	 * io.DataInputStream)
	 */
	@Override
	public abstract byte[] doProcess(DataInputStream dataInputStream) throws Exception;

}
