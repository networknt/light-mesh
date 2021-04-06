package com.networknt.mesh.kafka;

import com.networknt.server.ShutdownHookProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the shutdown hook for the callback consumer that is actively consume a kafka topic in a loop and
 * whenever a message is retrieved, it will invoke a REST endpoint on the backend Api/App. This will be used
 * to clean up the resource and end the consumer.
 *
 * @author Steve Hu
 */
public class CallbackConsumerShutdownHook implements ShutdownHookProvider {
    private static Logger logger = LoggerFactory.getLogger(CallbackConsumerShutdownHook.class);

    @Override
    public void onShutdown() {
        logger.debug("CallbackConsumerShutdownHook begins");
        if(CallbackConsumerStartupHook.done == false) {
            CallbackConsumerStartupHook.done = true;
        }
        logger.debug("CallbackConsumerShutdownHook ends");
    }
}
