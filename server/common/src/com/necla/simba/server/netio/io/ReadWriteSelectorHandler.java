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
 * Interface used for reading and writing from a socket using 
 * non-blocking operations.
 * 
 * Classes wishing to be notified when a socket is ready to be written
 * or read should implement this interface in order to receive 
 * notifications.
 * 
 * @author Nuno Santos
 */ 
public interface ReadWriteSelectorHandler extends SelectorHandler {  
  
  /**
   * Called when the associated socket is ready to be read.
   */
  public void handleRead();
  /**
   * Called when the associated socket is ready to be written.
   */  
  public void handleWrite();  
}