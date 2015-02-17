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
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import com.necla.simba.server.netio.handlers.ConnectorListener;
import com.necla.simba.server.netio.io.CallbackErrorHandler;
import com.necla.simba.server.netio.io.ConnectorSelectorHandler;
import com.necla.simba.server.netio.io.SelectorThread;

/**
 * Manages a non-blocking connection attempt to a remote host. 
 * 
 * @author Nuno Santos
 */
final public class Connector implements ConnectorSelectorHandler {
  // The socket being connected.
  private SocketChannel sc;  
  // The address of the remote endpoint.
  private final InetSocketAddress remoteAddress;
  // The selector used for receiving events.
  private final SelectorThread selectorThread;
  // The listener for the callback events.
  private final ConnectorListener listener;
  
  /**
   * Creates a new instance. The connection is not attempted here.
   * Use connect() to start the attempt.
   * 
   * @param remoteAddress The remote endpoint where to connect.
   * @param listener The object that will receive the callbacks from
   * this Connector.
   * @param selector The selector to be used.
   * @throws IOException
   */
  public Connector(SelectorThread selector, 
      InetSocketAddress remoteAddress, 
      ConnectorListener listener)
  {     
    this.selectorThread = selector;
    this.remoteAddress = remoteAddress;
    this.listener = listener;
  }
  
  /**
   * Starts a non-blocking connection attempt.
   *   
   * @throws IOException
   */
  public void connect() throws IOException {
    sc = SocketChannel.open();  
    // Very important. Set to non-blocking. Otherwise a call
    // to connect will block until the connection attempt fails 
    // or succeeds.
    sc.configureBlocking(false);
    sc.connect(remoteAddress);
    
    // Registers itself to receive the connect event.
    selectorThread.registerChannelLater(
        sc,
        SelectionKey.OP_CONNECT, 
        this,
        new CallbackErrorHandler() {
          public void handleError(Exception ex) {    
            listener.connectionFailed(Connector.this, ex);
          }
        });
  }
    
  /**
   * Called by the selector thread when the connection is 
   * ready to be completed.
   */
  public void handleConnect() {
    try {
      if (!sc.finishConnect()) {
        // Connection failed
        listener.connectionFailed(this, null);
        return;
      }
      // Connection succeeded
      listener.connectionEstablished(this, sc);
    } catch (IOException ex) {      
      // Could not connect.
      listener.connectionFailed(this, ex);
    }
  }
  
  public String toString() {   
    return "Remote endpoint: " + 
      remoteAddress.getAddress().getHostAddress() + ":" + remoteAddress.getPort();
  }
}