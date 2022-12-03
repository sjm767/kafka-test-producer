package com.jaeshim.kafka.test.producer.partitioner;

import com.jaeshim.kafka.test.producer.util.KafkaRackAwarenessUtil;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

public class RackAwarenessRoundRobinPartitioner implements Partitioner {

  private final static String clientRack=System.getenv("CLIENT_RACK");
  private final ConcurrentMap<String, AtomicInteger> topicCounterMap = new ConcurrentHashMap<>();

  public void configure(Map<String, ?> configs) {}

  @Override
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
    int numPartitions = partitions.size();
    int nextValue = nextValue(topic);

    List<PartitionInfo> availablePartitions = KafkaRackAwarenessUtil.getRackAwarenessAvailablePartitionsForTopic(cluster,topic,clientRack);

    if (!availablePartitions.isEmpty()) {
      int part = Utils.toPositive(nextValue) % availablePartitions.size();
      return availablePartitions.get(part).partition();
    } else {
      // no partitions are available, give a non-available partition
      return Utils.toPositive(nextValue) % numPartitions;
    }
  }

  private int nextValue(String topic) {
    AtomicInteger counter = topicCounterMap.computeIfAbsent(topic, k -> new AtomicInteger(0));
    return counter.getAndIncrement();
  }

  public void close() {}
}
