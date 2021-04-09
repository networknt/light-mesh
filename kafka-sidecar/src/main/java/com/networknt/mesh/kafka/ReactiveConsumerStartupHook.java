package com.networknt.mesh.kafka;

import com.networknt.client.Http2Client;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.exception.FrameworkException;
import com.networknt.kafka.common.KafkaConsumerConfig;
import com.networknt.kafka.consumer.*;
import com.networknt.kafka.entity.*;
import com.networknt.server.StartupHookProvider;
import io.undertow.UndertowOptions;
import io.undertow.client.ClientConnection;
import io.undertow.client.ClientRequest;
import io.undertow.client.ClientResponse;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.OptionMap;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class ReactiveConsumerStartupHook implements StartupHookProvider {
    private static Logger logger = LoggerFactory.getLogger(ReactiveConsumerStartupHook.class);
    static private KafkaConsumerConfig config = (KafkaConsumerConfig) Config.getInstance().getJsonObjectConfig(KafkaConsumerConfig.CONFIG_NAME, KafkaConsumerConfig.class);
    public static KafkaConsumerManager kafkaConsumerManager;
    static Http2Client client = Http2Client.getInstance();
    static private ClientConnection connection;
    static private ExecutorService executor = newSingleThreadExecutor();
    long timeoutMs = -1;
    long maxBytes = -1;
    String instanceId;
    // An indicator that will break the consumer loop so that the consume can be closed. It is set
    // by the CallbackConsumerShutdownHook to do the clean up.
    public static boolean done = false;

    @Override
    public void onStartup() {
        logger.debug("ReactiveConsumerStartupHook begins");
        // get or create the KafkaConsumerManager
        kafkaConsumerManager = new KafkaConsumerManager(config);
        String groupId = (String)config.getProperties().get("group.id");
        CreateConsumerInstanceRequest request = new CreateConsumerInstanceRequest(null, null, config.getValueFormat(), null, null, null, null);
        instanceId = kafkaConsumerManager.createConsumer(groupId, request.toConsumerInstanceConfig());

        String topic = config.getTopic();
        ConsumerSubscriptionRecord subscription;
        if(topic.contains(",")) {
            // remove the whitespaces
            topic = topic.replaceAll("\\s+","");
            subscription = new ConsumerSubscriptionRecord(Arrays.asList(topic.split(",", -1)), null);
        } else {
            subscription = new ConsumerSubscriptionRecord(Collections.singletonList(config.getTopic()), null);
        }
        kafkaConsumerManager.subscribe(groupId, instanceId, subscription);
        runConsumer();
        logger.debug("ReactiveConsumerStartupHook ends");
    }

    private void runConsumer() {
        executor.execute(new ConsumerTask());
        executor.shutdown();
    }

    class ConsumerTask implements Runnable {
        @Override
        public void run() {
            while (!done) {
                switch(config.getValueFormat()) {
                    case "binary":
                        readRecords(
                                (String)config.getProperties().get("group.id"),
                                instanceId,
                                Duration.ofMillis(timeoutMs),
                                maxBytes,
                                BinaryKafkaConsumerState.class,
                                BinaryConsumerRecord::fromConsumerRecord);
                        break;
                    case "json":
                        readRecords(
                                (String)config.getProperties().get("group.id"),
                                instanceId,
                                Duration.ofMillis(timeoutMs),
                                maxBytes,
                                JsonKafkaConsumerState.class,
                                JsonConsumerRecord::fromConsumerRecord);
                        break;
                    case "avro":
                    case "jsonschema":
                    case "protobuf":
                        readRecords(
                                (String)config.getProperties().get("group.id"),
                                instanceId,
                                Duration.ofMillis(timeoutMs),
                                maxBytes,
                                SchemaKafkaConsumerState.class,
                                SchemaConsumerRecord::fromConsumerRecord);
                        break;
                }
                try {
                    // wait a period of time before the next poll if there is no record
                    Thread.sleep(config.getWaitPeriod());
                } catch (InterruptedException e) {
                    logger.error("InterruptedException", e);
                    // ignore it.
                }
            }
        }
    }

    private <KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> void readRecords(
            String group,
            String instance,
            Duration timeout,
            long maxBytes,
            Class<? extends KafkaConsumerState<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT>>
                    consumerStateType,
            Function<com.networknt.kafka.entity.ConsumerRecord<ClientKeyT, ClientValueT>, ?> toJsonWrapper
    ) {
        maxBytes = (maxBytes <= 0) ? Long.MAX_VALUE : maxBytes;

        kafkaConsumerManager.readRecords(
                group, instance, consumerStateType, timeout, maxBytes,
                new ConsumerReadCallback<ClientKeyT, ClientValueT>() {
                    @Override
                    public void onCompletion(
                            List<ConsumerRecord<ClientKeyT, ClientValueT>> records, FrameworkException e
                    ) {
                        if (e != null) {
                            if(logger.isDebugEnabled()) logger.debug("FrameworkException:", e);

                        } else {
                            if(records.size() > 0) {
                                if(connection == null || !connection.isOpen()) {
                                    try {
                                        connection = client.borrowConnection(new URI("https://localhost:8444"), Http2Client.WORKER, client.getDefaultXnioSsl(), Http2Client.BUFFER_POOL, OptionMap.create(UndertowOptions.ENABLE_HTTP2, true)).get();
                                    } catch (Exception ex) {
//                                    Map<String, Object> firstRecord = list.get(0); // list is not empty at this point.
//                                    if(logger.isDebugEnabled()) logger.debug("Rollback to partition " + firstRecord.get("partition")  + " offset " + firstRecord.get("offset"), e);
//                                    consumer.seek(new TopicPartition((String)firstRecord.get("topic"), (Integer)firstRecord.get("partition")), (Long)firstRecord.get("offset"));
                                    }
                                }
                                final CountDownLatch latch = new CountDownLatch(1);
                                final AtomicReference<ClientResponse> reference = new AtomicReference<>();
                                try {
                                    ClientRequest request = new ClientRequest().setMethod(Methods.POST).setPath("/kafka/records");
                                    request.getRequestHeaders().put(Headers.CONTENT_TYPE, "application/json");
                                    request.getRequestHeaders().put(Headers.TRANSFER_ENCODING, "chunked");
                                    connection.sendRequest(request, client.createClientCallback(reference, latch, JsonMapper.toJson(records.stream().map(toJsonWrapper).collect(Collectors.toList()))));
                                    latch.await();
                                    int statusCode = reference.get().getResponseCode();
                                    String body = reference.get().getAttachment(Http2Client.RESPONSE_BODY);
                                    if(logger.isDebugEnabled()) logger.debug("statusCode = " + statusCode + " body  = " + body);
                                    if(statusCode >= 400) {
                                        // something happens on the backend and the data is not consumed correctly.
//                                    Map<String, Object> firstRecord = list.get(0); // list is not empty at this point.
//                                    if(logger.isDebugEnabled()) logger.debug("Rollback to partition " + firstRecord.get("partition")  + " offset " + firstRecord.get("offset"));
//                                    consumer.seek(new TopicPartition((String)firstRecord.get("topic"), (Integer)firstRecord.get("partition")), (Long)firstRecord.get("offset"));
                                    } else {
                                        //consumer.commitSync();
                                    }
                                } catch (Exception  exception) {
//                                Map<String, Object> firstRecord = list.get(0); // list is not empty at this point.
//                                if(logger.isDebugEnabled()) logger.debug("Rollback to partition " + firstRecord.get("partition")  + " offset " + firstRecord.get("offset"), e);
//                                consumer.seek(new TopicPartition((String)firstRecord.get("topic"), (Integer)firstRecord.get("partition")), (Long)firstRecord.get("offset"));
                                }
                            }
                        }
                    }
                }
        );
    }


}
