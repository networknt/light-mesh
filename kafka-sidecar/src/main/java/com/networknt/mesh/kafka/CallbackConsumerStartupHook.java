package com.networknt.mesh.kafka;

import com.networknt.client.Http2Client;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.kafka.common.KafkaConsumerConfig;
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
    public static final byte MAGIC_BYTE = 0x0;
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
        final Map<String, Object> properties = config.getProperties();
        // Create the consumer using properties.
        final Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties);
        // Subscribe to the topic.
        String topic = config.getTopic();
        if(topic.contains(",")) {
            // remove the whitespaces
            topic = topic.replaceAll("\\s+","");
            consumer.subscribe(Arrays.asList(topic.split(",", -1)));
        } else {
            consumer.subscribe(Collections.singletonList(config.getTopic()));
        }
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
                if (consumerRecords.count()==0) {
                    try {
                        // wait a period of time before the next poll if there is no record
                        Thread.sleep(config.getWaitPeriod());
                    } catch (InterruptedException e) {
                        logger.error("InterruptedException", e);
                        // ignore it.
                    }
                    continue;
                }

                List<Map<String, Object>> list = new ArrayList<>();
                Iterator<ConsumerRecord<byte[], byte[]>> recordIterator = consumerRecords.iterator();
                while(recordIterator.hasNext()) {
                    ConsumerRecord record = recordIterator.next();
                    Map<String, String> headerMap = new HashMap<>();
                    Iterator<Header> headerIterator = record.headers().iterator();
                    while(headerIterator.hasNext()) {
                        Header header = headerIterator.next();
                        headerMap.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
                    }
                    Map<String, Object> map = new HashMap<>();
                    map.put("topic", record.topic());
                    byte[] originalKey = (byte[])record.key();
                    // it is possible that the key does not exist.
                    if(originalKey != null) {
                        byte[] key = null;
                        if(originalKey[0] == MAGIC_BYTE) {
                            key = Arrays.copyOfRange(originalKey, 5, originalKey.length);
                        } else {
                            key = Arrays.copyOfRange(originalKey, 0, originalKey.length);
                        }
                        if (record.key() != null) map.put("key", new String(key, StandardCharsets.UTF_8));
                    }
                    byte[] originalValue = (byte[])record.value();
                    if(originalValue != null) {
                        byte[] value = null;
                        if(originalValue[0] == MAGIC_BYTE) {
                            value = Arrays.copyOfRange(originalValue, 5, originalValue.length);
                        } else {
                            value = Arrays.copyOfRange(originalValue, 0, originalValue.length);
                        }
                        if(record.value() != null) map.put("value", new String(value, StandardCharsets.UTF_8));
                    }
                    map.put("headers", headerMap);
                    map.put("partition", record.partition());
                    map.put("offset", record.offset());
                    list.add(map);
                }
                if(connection == null || !connection.isOpen()) {
                    try {
                        connection = client.borrowConnection(new URI("https://localhost:8444"), Http2Client.WORKER, client.getDefaultXnioSsl(), Http2Client.BUFFER_POOL, OptionMap.create(UndertowOptions.ENABLE_HTTP2, true)).get();
                    } catch (Exception e) {
                        Map<String, Object> firstRecord = list.get(0); // list is not empty at this point.
                        if(logger.isDebugEnabled()) logger.debug("Rollback to partition " + firstRecord.get("partition")  + " offset " + firstRecord.get("offset"), e);
                        consumer.seek(new TopicPartition((String)firstRecord.get("topic"), (Integer)firstRecord.get("partition")), (Long)firstRecord.get("offset"));
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
                        Map<String, Object> firstRecord = list.get(0); // list is not empty at this point.
                        if(logger.isDebugEnabled()) logger.debug("Rollback to partition " + firstRecord.get("partition")  + " offset " + firstRecord.get("offset"));
                        consumer.seek(new TopicPartition((String)firstRecord.get("topic"), (Integer)firstRecord.get("partition")), (Long)firstRecord.get("offset"));
                    } else {
                        consumer.commitSync();
                    }
                } catch (Exception  e) {
                    Map<String, Object> firstRecord = list.get(0); // list is not empty at this point.
                    if(logger.isDebugEnabled()) logger.debug("Rollback to partition " + firstRecord.get("partition")  + " offset " + firstRecord.get("offset"), e);
                    consumer.seek(new TopicPartition((String)firstRecord.get("topic"), (Integer)firstRecord.get("partition")), (Long)firstRecord.get("offset"));
                }
            }
            consumer.close();
        }
    }
}
