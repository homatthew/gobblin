package org.apache.gobblin.runtime.messaging;

import java.util.List;

/**
 * This buffer is used for sending messages between containers or physical nodes.
 * This interface provides the following:
 * <ul>
 *   <li>No guaranteed FIFO delivery</li>
 *   <li>Supports a single reader</li>
 *   <li>Safeguards for concurrent writers<br></li>
 *   <li>Messages delivered at most once</li>
 * </ul>
 */
public interface MessageBuffer<T> {
  /**
   * Add item to message buffer
   * @param item
   * @return item successfully added to buffer
   */
  boolean add(T item);

  /**
   * Get all messages from buffer. This is a destructive operation
   * @return All messages currently in the buffer. Empty list if no elements to get.
   */
  List<T> get();
}
