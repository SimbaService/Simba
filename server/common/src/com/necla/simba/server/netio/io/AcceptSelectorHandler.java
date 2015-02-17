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
 * Interface used for accepting incoming connections using non-blocking
 * operations.
 * 
 * Classes wishing to be notified when a ServerSocket receives incoming 
 * connections should implement this interface.
 * 
 * @author Nuno Santos 
 */
public interface AcceptSelectorHandler extends SelectorHandler {
  /**
   * Called by SelectorThread when the server socket associated
   * with the class implementing this interface receives a request
   * for establishing a connection.
   */
  public void handleAccept();
}
