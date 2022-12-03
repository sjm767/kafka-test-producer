package com.jaeshim.kafka.test.producer.partitioner;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

public class BugFixRoundRobinPartitioner implements Partitioner {

  /**
   * The "Round-Robin" partitioner - MODIFIED TO WORK PROPERLY WITH STICKY PARTITIONING (KIP-480)
   * <p>
   * This partitioning strategy can be used when user wants to distribute the writes to all
   * partitions equally. This is the behaviour regardless of record key hash.
   */
//    private static final Logger LOGGER = LoggerFactory.getLogger(RoundRobinPartitioner.class);
  private final ConcurrentMap<String, AtomicInteger> topicCounterMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Queue<Integer>> topicPartitionQueueMap = new ConcurrentHashMap<>();

  public void configure(Map<String, ?> configs) {
  }

  /**
   * Compute the partition for the given record.
   *
   * @param topic      The topic name
   * @param key        The key to partition on (or null if no key)
   * @param keyBytes   serialized key to partition on (or null if no key)
   * @param value      The value to partition on or null
   * @param valueBytes serialized value to partition on or null
   * @param cluster    The current cluster metadata
   */
  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    Queue<Integer> partitionQueue = partitionQueueComputeIfAbsent(topic);
    Integer queuedPartition = partitionQueue.poll();
    if (queuedPartition != null) {
//            LOGGER.trace("Partition chosen from queue: {}", queuedPartition);
      return queuedPartition;
    } else {
      List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
      int numPartitions = partitions.size();
      int nextValue = nextValue(topic);
      List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
      if (!availablePartitions.isEmpty()) {
        int part = Utils.toPositive(nextValue) % availablePartitions.size();
        int partition = availablePartitions.get(part).partition();
//                LOGGER.trace("Partition chosen: {}", partition);
        return partition;
      } else {
        // no partitions are available, give a non-available partition
        return Utils.toPositive(nextValue) % numPartitions;
      }
    }
  }

  private int nextValue(String topic) {
    AtomicInteger counter =
        topicCounterMap.computeIfAbsent(
            topic,
            k -> {
              return new AtomicInteger(0);
            });
    return counter.getAndIncrement();
  }

  private Queue<Integer> partitionQueueComputeIfAbsent(String topic) {
    return topicPartitionQueueMap.computeIfAbsent(topic, k -> {
      return new ConcurrentLinkedQueue<>();
    });
  }

  public void close() {
  }

  /**
   * Notifies the partitioner a new batch is about to be created. When using the sticky partitioner,
   * this method can change the chosen sticky partition for the new batch.
   *
   * @param topic         The topic name
   * @param cluster       The current cluster metadata
   * @param prevPartition The partition previously selected for the record that triggered a new
   *                      batch
   */
  @Override
  public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
//        LOGGER.trace("New batch so enqueuing partition {} for topic {}", prevPartition, topic);
    Queue<Integer> partitionQueue = partitionQueueComputeIfAbsent(topic);
    partitionQueue.add(prevPartition);
  }
}