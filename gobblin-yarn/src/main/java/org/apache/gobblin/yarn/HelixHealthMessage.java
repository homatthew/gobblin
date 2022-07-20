package org.apache.gobblin.yarn;

import java.util.UUID;
import org.apache.helix.model.Message;


public class HelixHealthMessage extends Message {
  private static final String HEALTH_MSG = HelixMessageSubTypes.HEALTH.toString();

  private static final String SIMPLE_FIELD_SPLIT_AMT = "GOBBLIN_SPLIT_AMT";

  public HelixHealthMessage(String instanceId) {
    super(MessageType.USER_DEFINE_MSG, UUID.randomUUID().toString());
    setMsgSubType(HEALTH_MSG);
    setMsgState(MessageState.NEW);
    // Give it infinite time to process the message, as long as session is alive
    setExecutionTimeout(-1);
    getRecord().setSimpleField(SIMPLE_FIELD_SPLIT_AMT, instanceId);
  }

  /**
   * @param message The incoming message that has been received from helix.
   * @throws IllegalArgumentException if the message is not of right sub-type
   */
  public HelixHealthMessage(final Message message) {
    super(message.getRecord());
    if (!message.getMsgSubType().equals(HEALTH_MSG)) {
      throw new IllegalArgumentException("Invalid message subtype:" + message.getMsgSubType());
    }
  }

  public String getInstanceId() {
    return getRecord().getSimpleField(SIMPLE_FIELD_SPLIT_AMT);
  }
}
