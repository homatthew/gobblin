package org.apache.gobblin.util.event;

import com.google.common.eventbus.EventBus;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;


@AllArgsConstructor
@Getter
@Setter
@ToString
public class AddWorkUnitIdEvent {
  private MetadataBasedEvent event;
  private EventBus eventBus;
}
