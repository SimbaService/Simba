/*
 * Copyright 2004 WIT-Software, Lda. 
 * - web: http://www.wit-software.com 
 * - email: info@wit-software.com
 *
 * All rights reserved. Relased under terms of the 
 * Creative Commons' Attribution-NonCommercial-ShareAlike license.
 */
package com.necla.simba.server.netio.handlers;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import com.necla.simba.server.netio.handlers.Channel;
import com.necla.simba.server.netio.handlers.ChannelFactory;
import com.necla.simba.server.netio.handlers.ChannelListener;
import com.necla.simba.server.netio.handlers.PlainChannel;
import com.necla.simba.server.netio.io.SelectorThread;

/**
 * @author Nuno Santos
 */
public class PlainChannelFactory implements ChannelFactory {

	public Channel createChannel(SocketChannel sc, SelectorThread st, ChannelListener l) 
	throws IOException 
	{
		return new PlainChannel(st, sc, l);
	}

}
