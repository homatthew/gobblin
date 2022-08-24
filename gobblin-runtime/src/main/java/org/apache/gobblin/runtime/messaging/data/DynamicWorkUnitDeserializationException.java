package org.apache.gobblin.runtime.messaging.data;

/**
 * An exception when {@link DynamicWorkUnitMessage} cannot be correctly deserialized from underlying message storage
 */
public class DynamicWorkUnitDeserializationException extends RuntimeException {
  public DynamicWorkUnitDeserializationException(String message) {
    super(message);
  }
}
