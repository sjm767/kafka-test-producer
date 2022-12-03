package com.jaeshim.kafka.test.producer.util;

import io.micrometer.core.instrument.util.StringUtils;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

public final class KafkaRackAwarenessUtil {

  public static List<PartitionInfo> getRackAwarenessAvailablePartitionsForTopic(Cluster cluster, String topic, String clientRack){
    List<PartitionInfo> partitionInfos = cluster.availablePartitionsForTopic(topic);

    if(!partitionInfos.isEmpty() && !StringUtils.isEmpty(clientRack)){
        List<PartitionInfo> rackPartitionInfos = partitionInfos.stream().filter(x->x.leader().rack().equalsIgnoreCase(clientRack)).collect(
            Collectors.toList());

        if(!rackPartitionInfos.isEmpty()){
          partitionInfos = rackPartitionInfos;
        }
    }

    return partitionInfos;
  }

  private KafkaRackAwarenessUtil() {
  }
}
