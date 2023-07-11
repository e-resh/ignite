package org.apache.ignite.cache.query;

import org.apache.ignite.IgniteCheckedException;

/**
 * The exception is thrown if a query was timed out while executing on client side while pending server response.
 */
public class QueryClientTimeoutException extends IgniteCheckedException {
  /** */
  private static final long serialVersionUID = 0L;

  /** Error message. */
  private static final String ERR_MSG = "The query was timed out while executing timeout:";

  /**
   * Default constructor.
   */
  public QueryClientTimeoutException(int timeout) {
    super(ERR_MSG + timeout);
  }

}
