package com.networknt.mesh.kafka.handler;

import com.networknt.body.BodyHandler;
import com.networknt.config.JsonMapper;
import com.networknt.handler.LightHttpHandler;
import io.undertow.server.HttpServerExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ConsumerTopicGetHandler implements LightHttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerTopicGetHandler.class);
    private static String STATUS_ACCEPTED = "SUC10202";

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        if(logger.isDebugEnabled()) logger.debug("ConsumerTopicGetHandler start");
        // the topic is the path parameter, so it is required and cannot be null.
        String topic = exchange.getQueryParameters().get("topic").getFirst();
        logger.info("topic: " + topic);
        setExchangeStatus(exchange, STATUS_ACCEPTED);
    }

}
