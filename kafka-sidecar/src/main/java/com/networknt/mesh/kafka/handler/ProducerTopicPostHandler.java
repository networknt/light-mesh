package com.networknt.mesh.kafka.handler;

import com.fasterxml.jackson.databind.node.NullNode;
import com.google.protobuf.ByteString;
import com.networknt.body.BodyHandler;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.handler.LightHttpHandler;
import com.networknt.httpstring.AttachmentConstants;
import com.networknt.httpstring.HttpStringConstants;
import com.networknt.kafka.common.KafkaProducerConfig;
import com.networknt.kafka.producer.*;
import com.networknt.mesh.kafka.ProducerStartupHook;
import com.networknt.service.SingletonServiceFactory;
import com.networknt.utility.Constants;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import io.undertow.server.HttpServerExchange;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

/**
 * The producer endpoint that can receive request from the backend service messages and push them
 * into a kafka topic as path parameter in a transaction. Only when the message is successfully
 * acknowledged from Kafka, the response will be sent to the caller. This will guarantee that no
 * message will be missed in the process. However, due to the network issue, sometimes, the ack
 * might not received by the caller after the messages are persisted. So duplicated message might
 * be received, the handle will have a queue to cache the last several messages to remove duplicated
 * message possible.
 *
 * @author Steve Hu
 */
public class ProducerTopicPostHandler implements LightHttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(ProducerTopicPostHandler.class);
    private static String STATUS_ACCEPTED = "SUC10202";

    private final SchemaManager schemaManager;
    private final SchemaRecordSerializer recordSerializer;
    private String callerId = "unknown";
    private KafkaProducerConfig config;

    public ProducerTopicPostHandler() {
        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(
                singletonList("http://localhost:8081"),
                100,
                Arrays.asList(
                        new AvroSchemaProvider(), new JsonSchemaProvider(), new ProtobufSchemaProvider()),
                emptyMap()
                );
        Map<String, Object> configs = new HashMap<>();
        configs.put("schema.registry.url", "http://localhost:8081");
        recordSerializer = new SchemaRecordSerializer(schemaRegistryClient,configs, configs, configs);
        schemaManager = new SchemaManagerImpl(schemaRegistryClient, new TopicNameStrategy());
        SidecarProducer lightProducer = (SidecarProducer) SingletonServiceFactory.getBean(NativeLightProducer.class);
        config = lightProducer.config;
        if(config.isInjectCallerId()) {
            Map<String, Object> serverConfig = Config.getInstance().getJsonMapConfigNoCache("server");
            if(serverConfig != null) {
                callerId = (String)serverConfig.get("serviceId");
            }
        }

    }


    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        if(logger.isDebugEnabled()) logger.debug("ProducerTopicPostHandler start");
        // the topic is the path parameter, so it is required and cannot be null.
        String topic = exchange.getQueryParameters().get("topic").getFirst();
        logger.info("topic: " + topic);
        // multiple messages in a list.
        exchange.dispatch(exchange.getConnection().getWorker(), () -> {
            Map<String, Object> map = (Map)exchange.getAttachment(BodyHandler.REQUEST_BODY);
            System.out.println("map = " + JsonMapper.toJson(map));
            ProduceRequest produceRequest = Config.getInstance().getMapper().convertValue(map, ProduceRequest.class);
            // populate the headers from HTTP headers.
            Headers headers = populateHeaders(exchange, config, topic);
            CompletableFuture<ProduceResponse> responseFuture =
                    produceWithSchema(produceRequest.getFormat(), topic, Optional.empty(), produceRequest, headers);
            responseFuture.whenCompleteAsync((response, throwable) -> {
                exchange.getResponseHeaders().put(io.undertow.util.Headers.CONTENT_TYPE, "application/json");
                exchange.getResponseSender().send(JsonMapper.toJson(response));
            });
        });
    }
    /*
    working
    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        if(logger.isDebugEnabled()) logger.debug("ProducerTopicPostHandler start");
        // the topic is the path parameter, so it is required and cannot be null.
        String topic = exchange.getQueryParameters().get("topic").getFirst();
        logger.info("topic: " + topic);
        // multiple messages in a list.
        exchange.dispatch();
        Map<String, Object> map = (Map)exchange.getAttachment(BodyHandler.REQUEST_BODY);
        System.out.println("map = " + JsonMapper.toJson(map));
        ProduceRequest produceRequest = Config.getInstance().getMapper().convertValue(map, ProduceRequest.class);
        CompletableFuture<ProduceResponse> responseFuture =
                produceWithSchema(produceRequest.getFormat(), topic, Optional.empty(), produceRequest);
        responseFuture.whenCompleteAsync((response, throwable) -> {
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
            exchange.getResponseSender().send(JsonMapper.toJson(response));
        });
    }
    */
    /*
    working
    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        if(logger.isDebugEnabled()) logger.debug("ProducerTopicPostHandler start");
        // the topic is the path parameter, so it is required and cannot be null.
        String topic = exchange.getQueryParameters().get("topic").getFirst();
        logger.info("topic: " + topic);
        exchange.getRequestReceiver().receiveFullString((exchange1, message) ->{
            ProduceRequest produceRequest = JsonMapper.fromJson(message, ProduceRequest.class);
            CompletableFuture<ProduceResponse> responseFuture =
                    produceWithSchema(produceRequest.getFormat(), topic, Optional.empty(), produceRequest);
            exchange1.dispatch();
            responseFuture.whenCompleteAsync((response, throwable) -> {
                exchange1.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                exchange1.getResponseSender().send(JsonMapper.toJson(response));
            });
        });
    }
    */

    final CompletableFuture<ProduceResponse> produceJsonSchema(
            EmbeddedFormat format,
            String topicName,
            Optional<Integer> partition,
            ProduceRequest request,
            Headers headers) {
        List<SerializedKeyAndValue> serialized =
                serialize(
                        format,
                        topicName,
                        partition,
                        /* keySchema= */ Optional.empty(),
                        /* valueSchema= */ Optional.empty(),
                        request.getRecords());

        List<CompletableFuture<ProduceResult>> resultFutures = doProduce(topicName, serialized, headers);

        return produceResultsToResponse(
                /* keySchema= */ Optional.empty(), /* valueSchema= */ Optional.empty(), resultFutures);
    }


    final CompletableFuture<ProduceResponse> produceWithSchema(
            EmbeddedFormat format,
            String topicName,
            Optional<Integer> partition,
            ProduceRequest request,
            Headers headers) {
        Optional<RegisteredSchema> keySchema =
                getSchema(
                        format,
                        topicName,
                        request.getKeySchemaId(),
                        request.getKeySchema(),
                        /* isKey= */ true);
        Optional<RegisteredSchema> valueSchema =
                getSchema(
                        format,
                        topicName,
                        request.getValueSchemaId(),
                        request.getValueSchema(),
                        /* isKey= */ false);

        List<SerializedKeyAndValue> serialized =
                serialize(
                        format,
                        topicName,
                        partition,
                        keySchema,
                        valueSchema,
                        request.getRecords());

        List<CompletableFuture<ProduceResult>> resultFutures = doProduce(topicName, serialized, headers);

        return produceResultsToResponse(keySchema, valueSchema, resultFutures);
    }

    private Optional<RegisteredSchema> getSchema(
            EmbeddedFormat format,
            String topicName,
            Optional<Integer> schemaId,
            Optional<String> schema,
            boolean isKey) {
        if (format.requiresSchema() && (schemaId.isPresent() || schema.isPresent())) {
            return Optional.of(
                    schemaManager.getSchema(
                            /* topicName= */ topicName,
                            /* format= */ schema.map(unused -> format),
                            /* subject= */ Optional.empty(),
                            /* subjectNameStrategy= */ Optional.empty(),
                            /* schemaId= */ schemaId,
                            /* schemaVersion= */ Optional.empty(),
                            /* rawSchema= */ schema,
                            /* isKey= */ isKey));
        } else {
            return Optional.empty();
        }
    }

    private List<SerializedKeyAndValue> serialize(
            EmbeddedFormat format,
            String topicName,
            Optional<Integer> partition,
            Optional<RegisteredSchema> keySchema,
            Optional<RegisteredSchema> valueSchema,
            List<ProduceRecord> records) {

        return records.stream()
                .map(
                        record ->
                                new SerializedKeyAndValue(
                                        record.getPartition().map(Optional::of).orElse(partition),
                                        recordSerializer
                                                .serialize(
                                                        format,
                                                        topicName,
                                                        keySchema,
                                                        record.getKey().orElse(NullNode.getInstance()),
                                                        /* isKey= */ true),
                                        recordSerializer
                                                .serialize(
                                                        format,
                                                        topicName,
                                                        valueSchema,
                                                        record.getValue().orElse(NullNode.getInstance()),
                                                        /* isKey= */ false)))
                .collect(Collectors.toList());
    }

    private List<CompletableFuture<ProduceResult>> doProduce(
            String topicName, List<SerializedKeyAndValue> serialized, Headers headers) {
        return serialized.stream()
                .map(
                        record -> produce(
                                topicName,
                                record.getPartitionId(),
                                headers,
                                record.getKey(),
                                record.getValue(),
                                /* timestamp= */ Instant.now()))
                .collect(Collectors.toList());
    }

    private static CompletableFuture<ProduceResponse> produceResultsToResponse(
            Optional<RegisteredSchema> keySchema,
            Optional<RegisteredSchema> valueSchema,
            List<CompletableFuture<ProduceResult>> resultFutures
    ) {
        CompletableFuture<List<PartitionOffset>> offsetsFuture =
                CompletableFutures.allAsList(
                        resultFutures.stream()
                                .map(
                                        future ->
                                                future.thenApply(
                                                        result ->
                                                                new PartitionOffset(
                                                                        result.getPartitionId(),
                                                                        result.getOffset(),
                                                                        /* errorCode= */ null,
                                                                        /* error= */ null)))
                                .map(
                                        future ->
                                                future.exceptionally(
                                                        throwable ->
                                                                new PartitionOffset(
                                                                        /* partition= */ null,
                                                                        /* offset= */ null,
                                                                        errorCodeFromProducerException(throwable.getCause()),
                                                                        throwable.getCause().getMessage())))
                                .collect(Collectors.toList()));

        return offsetsFuture.thenApply(
                offsets ->
                        new ProduceResponse(
                                offsets,
                                keySchema.map(RegisteredSchema::getSchemaId).orElse(null),
                                valueSchema.map(RegisteredSchema::getSchemaId).orElse(null)));
    }

    private static int errorCodeFromProducerException(Throwable e) {
        if (e instanceof AuthenticationException) {
            return ProduceResponse.KAFKA_AUTHENTICATION_ERROR_CODE;
        } else if (e instanceof AuthorizationException) {
            return ProduceResponse.KAFKA_AUTHORIZATION_ERROR_CODE;
        } else if (e instanceof RetriableException) {
            return ProduceResponse.KAFKA_RETRIABLE_ERROR_ERROR_CODE;
        } else if (e instanceof KafkaException) {
            return ProduceResponse.KAFKA_ERROR_ERROR_CODE;
        } else {
            // We shouldn't see any non-Kafka exceptions, but this covers us in case we do see an
            // unexpected error. In that case we fail the entire request -- this loses information
            // since some messages may have been produced correctly, but is the right thing to do from
            // a REST perspective since there was an internal error with the service while processing
            // the request.
            logger.error("Unexpected Producer Exception", e);
            throw new RuntimeException("Unexpected Producer Exception", e);
        }
    }

    public Headers populateHeaders(HttpServerExchange exchange, KafkaProducerConfig config, String topic) {
        Headers headers = new RecordHeaders();
        String token = exchange.getRequestHeaders().getFirst(Constants.AUTHORIZATION_STRING);
        if(token != null) {
            headers.add(Constants.AUTHORIZATION_STRING, token.getBytes(StandardCharsets.UTF_8));
        }
        if(config.isInjectOpenTracing()) {
            Tracer tracer = exchange.getAttachment(AttachmentConstants.EXCHANGE_TRACER);
            if(tracer != null && tracer.activeSpan() != null) {
                Tags.SPAN_KIND.set(tracer.activeSpan(), Tags.SPAN_KIND_PRODUCER);
                Tags.MESSAGE_BUS_DESTINATION.set(tracer.activeSpan(), topic);
                tracer.inject(tracer.activeSpan().context(), Format.Builtin.TEXT_MAP, new KafkaHeadersCarrier(headers));
            }
        } else {
            String cid = exchange.getRequestHeaders().getFirst(HttpStringConstants.CORRELATION_ID);
            headers.add(Constants.CORRELATION_ID_STRING, cid.getBytes(StandardCharsets.UTF_8));
            String tid = exchange.getRequestHeaders().getFirst(HttpStringConstants.TRACEABILITY_ID);
            if(tid != null) {
                headers.add(Constants.TRACEABILITY_ID_STRING, tid.getBytes(StandardCharsets.UTF_8));
            }
        }
        if(config.isInjectCallerId()) {
            headers.add(Constants.CALLER_ID_STRING, callerId.getBytes(StandardCharsets.UTF_8));
        }
        return headers;
    }

    public CompletableFuture<ProduceResult> produce(
            String topicName,
            Optional<Integer> partitionId,
            Headers headers,
            Optional<ByteString> key,
            Optional<ByteString> value,
            Instant timestamp
    ) {
        CompletableFuture<ProduceResult> result = new CompletableFuture<>();
        ProducerStartupHook.producer.send(
                new ProducerRecord<>(
                        topicName,
                        partitionId.orElse(null),
                        timestamp.toEpochMilli(),
                        key.map(ByteString::toByteArray).orElse(null),
                        value.map(ByteString::toByteArray).orElse(null),
                        headers),
                (metadata, exception) -> {
                    if (exception != null) {
                        result.completeExceptionally(exception);
                    } else {
                        result.complete(ProduceResult.fromRecordMetadata(metadata));
                    }
                });
        return result;
    }

}
