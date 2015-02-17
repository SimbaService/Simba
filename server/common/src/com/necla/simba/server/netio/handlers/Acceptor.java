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
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import com.necla.simba.server.netio.handlers.AcceptorListener;
import com.necla.simba.server.netio.io.AcceptSelectorHandler;
import com.necla.simba.server.netio.io.CallbackErrorHandler;
import com.necla.simba.server.netio.io.SelectorThread;

/**
 * Listens for incoming connections from clients, using a selector
 * to receive connect events. Therefore, instances of this class
 * don't have an associated thread. When a connection is established,
 * it notifies a listener using a callback.
 * 
 * @author Nuno Santos
 */
final public class Acceptor implements AcceptSelectorHandler {
  // Used to receive incoming connections
  private ServerSocketChannel ssc; 
  // The selector used by this instance.
  private final SelectorThread ioThread;
  // Port where to listen for connections.
  private final int listenPort;  
  // Listener to be notified of new connections and of errors.
  private final AcceptorListener listener;

  /**
	 * Creates a new instance. No server socket is created. Use 
   * openServerSocket() to start listening.
	 * 
   * @param listenPort The port to open.
	 * @param listener The object that will receive notifications 
   *  of incoming connections.
	 * @param ioThread The selector thread.
	 * 
	 * @throws IOException
	 */
  public Acceptor(
     int listenPort,
    SelectorThread ioThread,
    AcceptorListener listener)    
  { 
    this.ioThread = ioThread;
    this.listenPort = listenPort;
    this.listener = listener;
  }
  
  /**
   * Starts listening for incoming connections. This method does 
   * not block waiting for connections. Instead, it registers itself 
   * with the selector to receive connect events.
   * 
   * @throws IOException
   */
  public void openServerSocket() throws IOException {
    ssc = ServerSocketChannel.open();
    InetSocketAddress isa = new InetSocketAddress(listenPort);
    ssc.socket().bind(isa, 100);
    
    // This method might be called from any thread. We must use 
    // the xxxLater methods so that the actual register operation
    // is done by the selector's thread. No other thread should access
    // the selector directly.
    ioThread.registerChannelLater(ssc,
        SelectionKey.OP_ACCEPT,
        this,
        new CallbackErrorHandler() {
      public void handleError(Exception ex) {    
        listener.socketError(Acceptor.this, ex);
      }
    });
  }
  
  public String toString() {  
    return "ListenPort: " + listenPort;
  }
  
  /**
   * Called by SelectorThread when the underlying server socket is 
   * ready to accept a connection. This method should not be called
   * from anywhere else.
   */
  public void handleAccept() {
    SocketChannel sc = null;
    try {
      sc = ssc.accept();
      Socket s = sc.socket();
      
      // Reactivate interest to receive the next connection. We
      // can use one of the XXXNow methods since this method is being
      // executed on the selector's thread.
      ioThread.addChannelInterestNow(ssc, SelectionKey.OP_ACCEPT);
   		s.setTcpNoDelay(true);
   		s.setKeepAlive(true);
    } catch (IOException e) {
      listener.socketError(this, e);
    }
    if (sc != null) {
      // Connection established
      listener.socketConnected(this, sc);
    }
  }
  
  /**
   * Closes the socket. Returns only when the socket has been
   * closed.
   */
  public void close()  {
    try {
      // Must wait for the socket to be closed.
      ioThread.invokeAndWait(new Runnable() {      
        public void run() {
          if (ssc != null) {
            try {
              ssc.close();
            } catch (IOException e) {
              // Ignore
            }
          }        
        }
      });
    } catch (InterruptedException e) {
      // Ignore
    }
  }  
}