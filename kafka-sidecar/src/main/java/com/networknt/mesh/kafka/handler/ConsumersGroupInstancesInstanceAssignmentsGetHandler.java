package com.networknt.mesh.kafka.handler;

import com.networknt.handler.LightHttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;

import java.util.Deque;
import java.util.Map;

/**
For more information on how to write business handlers, please check the link below.
https://doc.networknt.com/development/business-handler/rest/
*/
public class ConsumersGroupInstancesInstanceAssignmentsGetHandler implements LightHttpHandler {
    public ConsumersGroupInstancesInstanceAssignmentsGetHandler () {
    }

    
    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        Map<String, Deque<String>> queryParameters = exchange.getQueryParameters();
        Map<String, Deque<String>> pathParameters = exchange.getPathParameters();
        exchange.setStatusCode(200);
        exchange.getResponseSender().send("");
    }
}
