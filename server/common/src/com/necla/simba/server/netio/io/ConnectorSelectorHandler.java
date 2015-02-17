/*
 * Copyright 2004 WIT-Software, Lda. 
 * - web: http://www.wit-software.com 
 * - email: info@wit-software.com
 *
 * All rights reserved. Relased under terms of the 
 * Creative Commons' Attribution-NonCommercial-ShareAlike license.
 */
package com.necla.simba.server.netio.io;

import com.necla.simba.server.netio.io.SelectorHandler;

/**
 * Interface used for establishment a connection using non-blocking
 * operations.
 * 
 * Should be implemented by classes wishing to be notified 
 * when a Socket finishes connecting to a remote point. 
 * 
 * @author Nuno Santos
 */
public interface ConnectorSelectorHandler extends SelectorHandler {
  /**
   * Called by SelectorThread when the socket associated with the 
   * class implementing this interface finishes establishing a 
   * connection.
   */
  public void handleConnect();
}
