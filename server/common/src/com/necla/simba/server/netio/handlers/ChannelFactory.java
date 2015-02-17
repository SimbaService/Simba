/*
 * Copyright 2004 WIT-Software, Lda. 
 * - web: http://www.wit-software.com 
 * - email: info@wit-software.com
 *
 * All rights reserved. Relased under terms of the 
 * Creative Commons' Attribution-NonCommercial-ShareAlike license.
 */
package com.necla.simba.server.netio.handlers;

import com.necla.simba.server.netio.handlers.Channel;
import com.necla.simba.server.netio.handlers.ChannelListener;
import com.necla.simba.server.netio.io.SelectorThread;

import java.nio.channels.SocketChannel;

/**
 * @author Nuno Santos
 */
public interface ChannelFactory {
	
	public Channel createChannel(
			SocketChannel sc, 
			SelectorThread st, 
			ChannelListener l) throws Exception;
}
