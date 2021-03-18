package com.networknt.mesh.kafka.handler;

import com.networknt.body.BodyHandler;
import com.networknt.config.Config;
import com.networknt.handler.LightHttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import java.util.Deque;
import java.util.Map;

/**
For more information on how to write business handlers, please check the link below.
https://doc.networknt.com/development/business-handler/rest/
*/
public class ConsumersGroupInstancesInstancePositionsFirstPostHandler implements LightHttpHandler {

    public ConsumersGroupInstancesInstancePositionsFirstPostHandler () {
    }

    
    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        HeaderMap requestHeaders = exchange.getRequestHeaders();
        Map<String, Deque<String>> queryParameters = exchange.getQueryParameters();
        Map<String, Deque<String>> pathParameters = exchange.getPathParameters();
        exchange.setStatusCode(200);
        exchange.getResponseSender().send("");
    }
}
