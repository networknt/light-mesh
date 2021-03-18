package com.networknt.mesh.kafka;

import com.networknt.config.Config;
import com.networknt.kafka.common.KafkaConsumerConfig;
import com.networknt.kafka.consumer.KafkaConsumerManager;
import com.networknt.server.StartupHookProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerStartupHook  implements StartupHookProvider {
    private static Logger logger = LoggerFactory.getLogger(ConsumerStartupHook.class);
    public static KafkaConsumerManager kafkaConsumerManager;
    @Override
    public void onStartup() {
        logger.debug("ConsumerStartupHook begins");
        KafkaConsumerConfig config = (KafkaConsumerConfig) Config.getInstance().getJsonObjectConfig(KafkaConsumerConfig.CONFIG_NAME, KafkaConsumerConfig.class);
        kafkaConsumerManager = new KafkaConsumerManager(config);
        logger.debug("ConsumerStartupHook ends");
    }
}
