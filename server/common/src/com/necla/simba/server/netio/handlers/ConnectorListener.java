/*
 * Copyright 2004 WIT-Software, Lda. 
 * - web: http://www.wit-software.com 
 * - email: info@wit-software.com
 *
 * All rights reserved. Relased under terms of the 
 * Creative Commons' Attribution-NonCommercial-ShareAlike license.
 */
package com.necla.simba.server.netio.handlers;

import java.nio.channels.SocketChannel;

import com.necla.simba.server.netio.handlers.Connector;

/**
 * Callback interface for receiving events from a Connector. 
 * 
 * @author Nuno Santos
 */
public interface ConnectorListener {
  /**
   * Called when the connection is fully established. 
   * @param connector The source of this event. 
   * @param sc The newly connected socket.
   */
  public void connectionEstablished(Connector connector, SocketChannel sc);
  /**
   * Called when the connection fails to be established.
   * @param connector The source of this event.
   * @param cause The cause of the error.
   */
  public void connectionFailed(Connector connector, Exception cause);
}
