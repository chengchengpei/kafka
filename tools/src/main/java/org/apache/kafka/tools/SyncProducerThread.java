package org.apache.kafka.tools;

import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class SyncProducerThread implements Runnable {
    private final KafkaProducer<String, String> producer;
    private final String topicName;
    private final long NumRecordsPerThread;
    private final List<String> payloadList;
    private final Random random = new Random(0);
    private boolean transactionsEnabled = false;
    private int throughput = -1;
    private long transactionDurationMs = 0;

    public SyncProducerThread(int threadIndex,
            Properties props,
            String topicName,
            long NumRecordsPerThread,
            List<String> payloadList,
            int throughput,
            String transactionalId,
            long transactionDurationMs) {
        this.topicName = topicName;
        this.NumRecordsPerThread = NumRecordsPerThread;
        this.payloadList = payloadList;
        this.throughput = throughput;
        this.transactionDurationMs = transactionDurationMs;
        transactionsEnabled =  0 < transactionDurationMs;

        if (transactionsEnabled) {
            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId+"-"+threadIndex);
        }

        this.producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
    }
   
    @Override
    public void run() {
      if (transactionsEnabled) {
          producer.initTransactions();
      }
      
      long startMs = System.currentTimeMillis();
      
      ThroughputThrottler throttler = new ThroughputThrottler(throughput, startMs);
      
      int currentTransactionSize = 0;
      long transactionStartTime = 0;
      
      for (int i = 0; i < this.NumRecordsPerThread; i++) {
          if (transactionsEnabled && currentTransactionSize == 0) {
              producer.beginTransaction();
              transactionStartTime = System.currentTimeMillis();
          }
          
          long sendStartMs = System.currentTimeMillis();
          
          String payload = payloadList.get(random.nextInt(payloadList.size()));

          try {
              producer.send(new ProducerRecord<String, String>(topicName, payload)).get();
          } catch (InterruptedException | ExecutionException e) {
              e.printStackTrace();
          }
          
          currentTransactionSize++;
          if (transactionsEnabled && transactionDurationMs <= (sendStartMs - transactionStartTime)) {
              producer.commitTransaction();
              currentTransactionSize = 0;
          }

          if (throttler.shouldThrottle(i, sendStartMs)) {
              throttler.throttle();
          }
      }
      
      if (transactionsEnabled && currentTransactionSize != 0) {
          producer.commitTransaction();
      }
      
      // Flushes and closes producer
      producer.flush();
      producer.close();
    }
}
