package com.networknt.mesh.kafka.handler;

import com.networknt.body.BodyHandler;
import com.networknt.config.Config;
import com.networknt.exception.FrameworkException;
import com.networknt.handler.LightHttpHandler;
import com.networknt.kafka.entity.ConsumerSubscriptionRecord;
import com.networknt.mesh.kafka.ConsumerStartupHook;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.Map;

/**
For more information on how to write business handlers, please check the link below.
https://doc.networknt.com/development/business-handler/rest/
*/
public class ConsumersGroupInstancesInstanceSubscriptionsDeleteHandler implements LightHttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(ConsumersGroupInstancesInstanceDeleteHandler.class);
    private static final String CONSUMER_INSTANCE_NOT_FOUND = "ERR12200";

    public ConsumersGroupInstancesInstanceSubscriptionsDeleteHandler () {
        if(logger.isDebugEnabled()) logger.debug("ConsumersGroupInstancesInstanceSubscriptionsDeleteHandler constructed!");
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        String group = exchange.getPathParameters().get("group").getFirst();
        String instance = exchange.getPathParameters().get("instance").getFirst();
        if(logger.isDebugEnabled()) logger.debug("group = " + group + " instance = " + instance);
        try {
            ConsumerStartupHook.kafkaConsumerManager.unsubscribe(group, instance);
            exchange.setStatusCode(204);
            exchange.endExchange();
        } catch (FrameworkException e) {
            if(logger.isDebugEnabled()) logger.debug("FrameworkException:", e);
            setExchangeStatus(exchange, e.getStatus());
        }
    }
}
