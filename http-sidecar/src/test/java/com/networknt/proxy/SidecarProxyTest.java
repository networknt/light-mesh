/*
 * Copyright (c) 2016 Network New Technologies Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.networknt.proxy;

import com.networknt.client.Http2Client;
import com.networknt.config.Config;
import com.networknt.exception.ClientException;
import com.networknt.utility.StringUtils;
import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.client.ClientConnection;
import io.undertow.client.ClientRequest;
import io.undertow.client.ClientResponse;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.RoutingHandler;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import io.undertow.util.Methods;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.IoUtils;
import org.xnio.OptionMap;

import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class SidecarProxyTest {
    static final Logger logger = LoggerFactory.getLogger(SidecarProxyTest.class);

    static Undertow backend = null;

    @ClassRule
    public static TestServer server = TestServer.getInstance();

    static final boolean enableHttp2 = server.getServerConfig().isEnableHttp2();
    static final boolean enableHttps = server.getServerConfig().isEnableHttps();
    static final int httpPort = server.getServerConfig().getHttpPort();
    static final int httpsPort = server.getServerConfig().getHttpsPort();
    static final String url = enableHttp2 || enableHttps ? "https://localhost:" + httpsPort : "http://localhost:" + httpPort;

    @BeforeClass
    public static void setUp() {
        if (backend == null) {
            logger.info("starting backend server");
            backend = Undertow.builder()
                    .addHttpListener(8081, "localhost", ROUTES)
                    .build();
            backend.start();
        }

    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (backend != null) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {

            }
            backend.stop();
            logger.info("The backend server is stopped.");
        }
    }


    private static HttpHandler ROUTES = new RoutingHandler()
            .get("/get", new HttpHandler() {
                @Override
                public void handleRequest(HttpServerExchange exchange) throws Exception {
                    exchange.setStatusCode(200);
                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                    exchange.getResponseSender().send("{\"backend\":\"OK\"}");
                }
            })
            .post("/post", new HttpHandler() {
                @Override
                public void handleRequest(HttpServerExchange exchange) throws Exception {
                    if(exchange.isInIoThread()) {
                        exchange.dispatch(this);
                        return;
                    }
                    exchange.startBlocking();
                    InputStream inputStream = exchange.getInputStream();
                    String requestBody = StringUtils.inputStreamToString(inputStream, StandardCharsets.UTF_8);
                    exchange.setStatusCode(200);
                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                    exchange.getResponseSender().send(requestBody);
                }
            })
            .setFallbackHandler(new HttpHandler() {
                @Override
                public void handleRequest(HttpServerExchange exchange) throws Exception {
                    exchange.setStatusCode(404);
                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                    exchange.getResponseSender().send("{\"backend\":\"Not Found\"}");
                }
            });

    @Test
    public void testGet() throws Exception {
        final Http2Client client = Http2Client.getInstance();
        final CountDownLatch latch = new CountDownLatch(10);
        final ClientConnection connection;
        try {
            connection = client.connect(new URI(url), Http2Client.WORKER, Http2Client.SSL, Http2Client.BUFFER_POOL, enableHttp2 ? OptionMap.create(UndertowOptions.ENABLE_HTTP2, true): OptionMap.EMPTY).get();
        } catch (Exception e) {
            throw new ClientException(e);
        }
        final List<AtomicReference<ClientResponse>> references = new CopyOnWriteArrayList<>();
        try {
            connection.getIoThread().execute(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 10; i++) {
                        AtomicReference<ClientResponse> reference = new AtomicReference<>();
                        references.add(i, reference);
                        final ClientRequest request = new ClientRequest().setMethod(Methods.GET).setPath("/get");
                        connection.sendRequest(request, client.createClientCallback(reference, latch));
                    }
                }

            });

            latch.await();
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw new ClientException(e);
        } finally {
            IoUtils.safeClose(connection);
        }
        for (final AtomicReference<ClientResponse> reference : references) {
            Assert.assertTrue(reference.get().getAttachment(Http2Client.RESPONSE_BODY).contains("{\"backend\":\"OK\"}"));
            System.out.println(reference.get().getAttachment(Http2Client.RESPONSE_BODY));
        }
    }

    @Test
    public void testPost() throws Exception {
        final Http2Client client = Http2Client.getInstance();
        final CountDownLatch latch = new CountDownLatch(1);
        final ClientConnection connection;
        try {
            connection = client.connect(new URI(url), Http2Client.WORKER, Http2Client.SSL, Http2Client.BUFFER_POOL, enableHttp2 ? OptionMap.create(UndertowOptions.ENABLE_HTTP2, true): OptionMap.EMPTY).get();
        } catch (Exception e) {
            throw new ClientException(e);
        }
        final AtomicReference<ClientResponse> reference = new AtomicReference<>();
        try {
            ClientRequest request = new ClientRequest().setPath("/post").setMethod(Methods.POST);
            request.getRequestHeaders().put(Headers.CONTENT_TYPE, "application/json");
            request.getRequestHeaders().put(Headers.TRANSFER_ENCODING, "chunked");
            request.getRequestHeaders().put(new HttpString("host"), "localhost");
            connection.sendRequest(request, client.createClientCallback(reference, latch, "{\"proxy\": \"hello\"}"));
            latch.await();
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw new ClientException(e);
        } finally {
            IoUtils.safeClose(connection);
        }
        Assert.assertTrue(reference.get().getAttachment(Http2Client.RESPONSE_BODY).contains("{\"proxy\": \"hello\"}"));
        System.out.println(reference.get().getAttachment(Http2Client.RESPONSE_BODY));
    }

}