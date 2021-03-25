package com.networknt.mesh.kafka;

import com.networknt.client.Http2Client;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.kafka.common.KafkaConsumerConfig;
import com.networknt.kafka.consumer.KafkaConsumerManager;
import com.networknt.server.StartupHookProvider;
import io.undertow.UndertowOptions;
import io.undertow.client.ClientConnection;
import io.undertow.client.ClientRequest;
import io.undertow.client.ClientResponse;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.OptionMap;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

/**
 * This is the callback consumer that is actively consume a kafka topic in a loop and whenever a message is
 * retrieved, it will invoke a REST endpoint on the backend Api/App.
 *
 * @author Steve Hu
 */
public class CallbackConsumerStartupHook implements StartupHookProvider {
    private static Logger logger = LoggerFactory.getLogger(CallbackConsumerStartupHook.class);
    // An indicator that will break the consumer loop so that the consume can be closed. It is set
    // by the CallbackConsumerShutdownHook to do the clean up.
    public static boolean done = false;
    public static KafkaConsumerManager kafkaConsumerManager;

    static Http2Client client = Http2Client.getInstance();
    static private ClientConnection connection;
    static private ExecutorService executor = newSingleThreadExecutor();
    static private KafkaConsumerConfig config = (KafkaConsumerConfig) Config.getInstance().getJsonObjectConfig(KafkaConsumerConfig.CONFIG_NAME, KafkaConsumerConfig.class);

    @Override
    public void onStartup() {
        logger.debug("CallbackConsumerStartupHook begins");
        runConsumer();
        logger.debug("CallbackConsumerStartupHook ends");
    }

    private Consumer<byte[], byte[]> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupId());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, config.getKeyDeserializer());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, config.getValueDeserializer());

        // Create the consumer using props.
        final Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(config.getTopic()));
        return consumer;
    }

    private void runConsumer() {
        executor.execute(new ConsumerTask());
        executor.shutdown();
    }

    class ConsumerTask implements Runnable {
        @Override
        public void run() {
            Consumer<byte[], byte[]> consumer = createConsumer();
            while (!done) {
                final ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofMillis(100));
                int partition = 0;
                long offset = 0;
                if (consumerRecords.count()==0) {
                    try {
                        // wait 10 seconds before the next poll. TODO make it configurable.
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        logger.error("InterruptedException", e);
                        // ignore it.
                    }
                    continue;
                }

                List<Map<String, Object>> list = new ArrayList<>();
                Iterator<ConsumerRecord<byte[], byte[]>> recordIterator = consumerRecords.iterator();
                boolean firstRecord = true;
                while(recordIterator.hasNext()) {
                    ConsumerRecord record = recordIterator.next();
                    if(firstRecord) {
                        // first record and we mark the partition and offset for rollback if exception occurs
                        partition = record.partition();
                        offset = record.offset();
                        firstRecord = false;
                    }
                    Map<String, String> headerMap = new HashMap<>();
                    Iterator<Header> headerIterator = record.headers().iterator();
                    while(headerIterator.hasNext()) {
                        Header header = headerIterator.next();
                        headerMap.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
                    }
                    Map<String, Object> map = new HashMap<>();
                    byte[] originalKey = (byte[])record.key();
                    byte[] key = Arrays.copyOfRange(originalKey, 5, originalKey.length);
                    if(record.key() != null) map.put("key", new String(key, StandardCharsets.UTF_8));
                    byte[] originalValue = (byte[])record.value();
                    byte[] value = Arrays.copyOfRange(originalValue, 5, originalValue.length);
                    if(record.value() != null) map.put("value", new String(value, StandardCharsets.UTF_8));
                    map.put("headers", headerMap);
                    map.put("partition", record.partition());
                    map.put("offset", record.offset());
                    list.add(map);
                }
                if(connection == null || !connection.isOpen()) {
                    try {
                        connection = client.borrowConnection(new URI("https://localhost:8444"), Http2Client.WORKER, client.getDefaultXnioSsl(), Http2Client.BUFFER_POOL, OptionMap.create(UndertowOptions.ENABLE_HTTP2, true)).get();
                    } catch (Exception e) {
                        if(logger.isDebugEnabled()) logger.debug("Rollback to partition " + partition  + " offset " + offset, e);
                        consumer.seek(new TopicPartition(config.getTopic(), partition), offset);
                    }
                }
                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicReference<ClientResponse> reference = new AtomicReference<>();
                try {
                    ClientRequest request = new ClientRequest().setMethod(Methods.POST).setPath("/kafka/records");
                    request.getRequestHeaders().put(Headers.CONTENT_TYPE, "application/json");
                    request.getRequestHeaders().put(Headers.TRANSFER_ENCODING, "chunked");
                    connection.sendRequest(request, client.createClientCallback(reference, latch, JsonMapper.toJson(list)));
                    latch.await();
                    int statusCode = reference.get().getResponseCode();
                    String body = reference.get().getAttachment(Http2Client.RESPONSE_BODY);
                    if(logger.isDebugEnabled()) logger.debug("statusCode = " + statusCode + " body  = " + body);
                    if(statusCode >= 400) {
                        // something happens on the backend and the data is not consumed correctly.
                        consumer.seek(new TopicPartition(config.getTopic(), partition), offset);
                    } else {
                        consumer.commitSync();
                    }
                } catch (Exception  e) {
                    if(logger.isDebugEnabled()) logger.debug("Rollback to partition " + partition  + " offset " + offset, e);
                    consumer.seek(new TopicPartition(config.getTopic(), partition), offset);
                }
            }
            consumer.close();
            System.out.println("DONE");

        }
    }
}
