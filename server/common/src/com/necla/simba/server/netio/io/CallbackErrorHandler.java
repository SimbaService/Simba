/*
 * Copyright 2004 WIT-Software, Lda. 
 * - web: http://www.wit-software.com 
 * - email: info@wit-software.com
 *
 * All rights reserved. Relased under terms of the 
 * Creative Commons' Attribution-NonCommercial-ShareAlike license.
 */
package com.necla.simba.server.netio.io;

/**
 * Defines the callback to be used to handle errors in 
 * asynchronous method invocations.
 * 
 * When an operation is executed asynchronously it is not
 * possible to use exceptions for error handling. Instead,
 * the caller must provide a callback to be used in case
 * of error. 
 * 
 * @author Nuno Santos 
 */
public interface CallbackErrorHandler {
  /**
   * Called when an exception is raised when executing an
   * asynchronous method. 
   * @param ex
   */
  public void handleError(Exception ex);
}
