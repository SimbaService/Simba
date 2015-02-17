/*
 * Copyright 2004 WIT-Software, Lda. 
 * - web: http://www.wit-software.com 
 * - email: info@wit-software.com
 *
 * All rights reserved. Relased under terms of the 
 * Creative Commons' Attribution-NonCommercial-ShareAlike license.
 */
package com.necla.simba.server.netio.handlers;

import com.necla.simba.server.netio.handlers.ChannelListener;
import com.necla.simba.server.netio.io.ReadWriteSelectorHandler;
import com.necla.simba.server.netio.io.SelectorThread;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * @author Nuno Santos
 */
public abstract class Channel implements ReadWriteSelectorHandler {
	protected final SelectorThread st;
	protected final SocketChannel sc;
	
	protected final ChannelListener listener;
	
	/**
	 * 
	 * @param st The selector to use to obtain IO events
	 * @param sc The channel to read/write
	 */
	public Channel(
			SelectorThread st, 
			SocketChannel sc,
			ChannelListener listener) {
		assert st != null;
		assert sc != null;
		assert listener != null;
		
		this.st = st;
		this.sc = sc;
		this.listener = listener;
	}

	/**
	 * Follows the contract of SocketChannel.read() for non-blocking
	 * operations
	 */
	public abstract int read(ByteBuffer bb) throws IOException; 
	
	/**
	 * Follows the contract of SocketChannel.write() for non-blocking
	 * operations	
	 */
	public abstract int write(ByteBuffer bb) throws IOException;

	public abstract void registerForRead() throws IOException;
	public abstract void unregisterForRead() throws IOException;
	
	public abstract void registerForWrite() throws IOException;
	public abstract void unregisterForWrite() throws IOException;

	public abstract void close() throws IOException;
	
	
	public SocketChannel getSocketChannel() {
		return sc;
	}
}
