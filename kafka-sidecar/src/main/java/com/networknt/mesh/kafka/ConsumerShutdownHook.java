package com.networknt.mesh.kafka;

import com.networknt.server.ShutdownHookProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerShutdownHook implements ShutdownHookProvider {
    private static Logger logger = LoggerFactory.getLogger(ConsumerShutdownHook.class);

    @Override
    public void onShutdown() {
        logger.debug("ConsumerShutdownHook begins");
        if(ConsumerStartupHook.kafkaConsumerManager != null) {
            ConsumerStartupHook.kafkaConsumerManager.shutdown();
        }
        logger.debug("ConsumerShutdownHook ends");
    }
}
