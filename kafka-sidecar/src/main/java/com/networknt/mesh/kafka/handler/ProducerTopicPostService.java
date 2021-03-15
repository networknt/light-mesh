package com.networknt.mesh.kafka.handler;

import com.google.protobuf.ByteString;
import com.networknt.kafka.producer.ProduceResult;
import com.networknt.kafka.producer.ProducerService;
import com.networknt.mesh.kafka.ProducerStartupHook;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class ProducerTopicPostService implements ProducerService {

    @Override
    public CompletableFuture<ProduceResult> produce(
            String topicName,
            Optional<Integer> partitionId,
            Map<String, Optional<ByteString>> headers,
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
                        headers.entrySet().stream()
                                .map(
                                        header ->
                                                new RecordHeader(
                                                        header.getKey(),
                                                        header.getValue().map(ByteString::toByteArray).orElse(null)))
                                .collect(Collectors.toList())),
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
