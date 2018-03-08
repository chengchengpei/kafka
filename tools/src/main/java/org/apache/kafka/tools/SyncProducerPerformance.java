/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.tools;

import static net.sourceforge.argparse4j.impl.Arguments.store;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.LinkedList;

import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

public class SyncProducerPerformance {

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = argParser();

        try {
            Namespace res = parser.parseArgs(args);

            /* Parse args */
            String topicName = res.getString("topic");
            long numRecordsPerThread = res.getLong("numRecordsPerThread");
            long numThreads = res.getLong("numThreads");
            Integer recordSize = res.getInt("recordSize");
            int throughput = res.getInt("throughput");
            List<String> producerProps = res.getList("producerConfig");
            String producerConfig = res.getString("producerConfigFile");
            String payloadFilePath = res.getString("payloadFile");
            String transactionalId = res.getString("transactionalId");
            long transactionDurationMs = res.getLong("transactionDurationMs");

            // since default value gets printed with the help text, we are escaping \n there and replacing it with correct value here.
            String payloadDelimiter = res.getString("payloadDelimiter").equals("\\n") ? "\n" : res.getString("payloadDelimiter");

            if (producerProps == null && producerConfig == null) {
                throw new ArgumentParserException("Either --producer-props or --producer.config must be specified.", parser);
            }
            
            /* Prepare payload */
            List<String> payloadList = new ArrayList<>();
            if (payloadFilePath != null) {
                Path path = Paths.get(payloadFilePath);
                System.out.println("Reading payloads from: " + path.toAbsolutePath());
                if (Files.notExists(path) || Files.size(path) == 0)  {
                    throw new  IllegalArgumentException("File does not exist or empty file provided.");
                }

                String[] payloads = new String(Files.readAllBytes(path), "UTF-8").split(payloadDelimiter);

                System.out.println("Number of messages read: " + payloads.length);
                for(String payload : payloads) {
                    payloadList.add(payload);
                }
            }

            if (recordSize != null) {
                Random random = new Random(0);
                byte[] payload = new byte[recordSize];
                for (int i = 0; i < payload.length; ++i) {
                    payload[i] = (byte) (random.nextInt(26) + 65);
                }
                System.out.println("Generated payload: " + payload);
                payloadList.add(new String(payload));
            }

            /* Prepare props for Kafka Producer */
            Properties props = new Properties();
            if (producerConfig != null) {
                props.putAll(Utils.loadProps(producerConfig));
            }
            if (producerProps != null) {
                for (String prop : producerProps) {
                    String[] pieces = prop.split("=");
                    if (pieces.length != 2)
                        throw new IllegalArgumentException("Invalid property: " + prop);
                    props.put(pieces[0], pieces[1]);
                }
            }

            /* Prepare threads for perf test */
            LinkedList<Thread> threads = new LinkedList<>();
            for (int i = 0; i < numThreads; i++) {
                Thread thread = new Thread(new SyncProducerThread(i, props, topicName,
                        numRecordsPerThread, payloadList,
                        throughput, transactionalId, transactionDurationMs));
                
                threads.add(thread);
            }
            
            long totalTimeMs = 0;
            long perfTestStartMs = System.currentTimeMillis();
            
            /* Start threads*/
            for(Thread thread : threads) {
                thread.start();
            }
            
            for(Thread thread : threads) {
                thread.join();
            }
            
            totalTimeMs = (System.currentTimeMillis() - perfTestStartMs);
            
            // TODO: should get and print more metrics
            /* Print throughput */
            System.out.println(String.format(
                    "Number of messages produced = %d, time = %d (ms), throughput = %.1f (messages per second)",
                    numRecordsPerThread * numThreads,
                    totalTimeMs,
                    (numRecordsPerThread * numThreads) / (totalTimeMs / 1000.0)));

        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                Exit.exit(0);
            } else {
                parser.handleError(e);
                Exit.exit(1);
            }
        }

    }

    /** Get the command-line argument parser. */
    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("producer-performance")
                .defaultHelp(true)
                .description("This tool is used to verify the producer performance.");

        MutuallyExclusiveGroup payloadOptions = parser
                .addMutuallyExclusiveGroup()
                .required(true)
                .description("either --record-size or --payload-file must be specified but not both.");

        parser.addArgument("--topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("produce messages to this topic");

        parser.addArgument("--num-records-per-thread")
                .action(store())
                .required(true)
                .type(Long.class)
                .metavar("NUM-RECORDS-PER-THREAD")
                .dest("numRecordsPerThread")
                .help("number of messages per thread to produce");
        
        parser.addArgument("--num-threads")
                .action(store())
                .required(true)
                .type(Long.class)
                .metavar("NUM-THREADS")
                .dest("numThreads")
                .help("number of threads to produce");

        payloadOptions.addArgument("--record-size")
                .action(store())
                .required(false)
                .type(Integer.class)
                .metavar("RECORD-SIZE")
                .dest("recordSize")
                .help("message size in bytes. Note that you must provide exactly one of --record-size or --payload-file.");

        payloadOptions.addArgument("--payload-file")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("PAYLOAD-FILE")
                .dest("payloadFile")
                .help("file to read the message payloads from. This works only for UTF-8 encoded text files. " +
                        "Payloads will be read from this file and a payload will be randomly selected when sending messages. " +
                        "Note that you must provide exactly one of --record-size or --payload-file.");

        parser.addArgument("--payload-delimiter")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("PAYLOAD-DELIMITER")
                .dest("payloadDelimiter")
                .setDefault("\\n")
                .help("provides delimiter to be used when --payload-file is provided. " +
                        "Defaults to new line. " +
                        "Note that this parameter will be ignored if --payload-file is not provided.");

        parser.addArgument("--throughput")
                .action(store())
                .required(true)
                .type(Integer.class)
                .metavar("THROUGHPUT")
                .help("throttle maximum message throughput to *approximately* THROUGHPUT messages/sec");

        parser.addArgument("--producer-props")
                 .nargs("+")
                 .required(false)
                 .metavar("PROP-NAME=PROP-VALUE")
                 .type(String.class)
                 .dest("producerConfig")
                 .help("kafka producer related configuration properties like bootstrap.servers,client.id etc. " +
                         "These configs take precedence over those passed via --producer.config.");

        parser.addArgument("--producer.config")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("CONFIG-FILE")
                .dest("producerConfigFile")
                .help("producer config properties file.");

        parser.addArgument("--transactional-id")
               .action(store())
               .required(false)
               .type(String.class)
               .metavar("TRANSACTIONAL-ID")
               .dest("transactionalId")
               .setDefault("performance-producer-default-transactional-id")
               .help("The transactionalId to use if transaction-duration-ms is > 0. Useful when testing the performance of concurrent transactions.");

        parser.addArgument("--transaction-duration-ms")
               .action(store())
               .required(false)
               .type(Long.class)
               .metavar("TRANSACTION-DURATION")
               .dest("transactionDurationMs")
               .setDefault(0L)
               .help("The max age of each transaction. The commitTransaction will be called after this this time has elapsed. Transactions are only enabled if this value is positive.");


        return parser;
    }
}
