package com.networknt.mesh.kafka.handler;

import com.networknt.client.Http2Client;
import com.networknt.exception.ClientException;
import com.networknt.mesh.kafka.TestServer;
import com.networknt.openapi.ResponseValidator;
import com.networknt.schema.SchemaValidatorsConfig;
import com.networknt.status.Status;
import io.undertow.UndertowOptions;
import io.undertow.client.ClientConnection;
import io.undertow.client.ClientRequest;
import io.undertow.client.ClientResponse;
import io.undertow.util.HeaderValues;
import io.undertow.util.HttpString;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.IoUtils;
import org.xnio.OptionMap;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;


@Ignore
public class ProducerTopicPostHandlerTest {
    @ClassRule
    public static TestServer server = TestServer.getInstance();

    static final Logger logger = LoggerFactory.getLogger(ProducerTopicPostHandlerTest.class);
    static final boolean enableHttp2 = server.getServerConfig().isEnableHttp2();
    static final boolean enableHttps = server.getServerConfig().isEnableHttps();
    static final int httpPort = server.getServerConfig().getHttpPort();
    static final int httpsPort = server.getServerConfig().getHttpsPort();
    static final String url = enableHttp2 || enableHttps ? "https://localhost:" + httpsPort : "http://localhost:" + httpPort;
    static final String JSON_MEDIA_TYPE = "application/json";

    /**
     * This request body only contains the records with three elements. The key is a string and the value
     * is a JSON object.
     *
     * @throws ClientException
     */
    @Test
    public void testProducerTopicPostHandlerTest() throws ClientException {

        final Http2Client client = Http2Client.getInstance();
        final CountDownLatch latch = new CountDownLatch(1);
        final ClientConnection connection;
        try {
            if(enableHttps) {
                connection = client.connect(new URI(url), Http2Client.WORKER, Http2Client.SSL, Http2Client.BUFFER_POOL, enableHttp2 ? OptionMap.create(UndertowOptions.ENABLE_HTTP2, true): OptionMap.EMPTY).get();
            } else {
                connection = client.connect(new URI(url), Http2Client.WORKER, Http2Client.BUFFER_POOL, OptionMap.EMPTY).get();
            }
        } catch (Exception e) {
            throw new ClientException(e);
        }
        final AtomicReference<ClientResponse> reference = new AtomicReference<>();
        String requestUri = "/producer/test1";
        String httpMethod = "post";
        String requestBody = "{\"key_schema_id\":1,\"value_schema_id\":2,\"records\":[{\"key\":\"alice\",\"value\":{\"count\":0}},{\"key\":\"john\",\"value\":{\"count\":1}},{\"key\":\"alex\",\"value\":{\"count\":2}}]}";
        try {
            ClientRequest request = new ClientRequest().setPath(requestUri).setMethod(Methods.POST);

            request.getRequestHeaders().put(Headers.CONTENT_TYPE, JSON_MEDIA_TYPE);
            request.getRequestHeaders().put(Headers.TRANSFER_ENCODING, "chunked");
            //customized header parameters
            request.getRequestHeaders().put(new HttpString("host"), "localhost");
            connection.sendRequest(request, client.createClientCallback(reference, latch, requestBody));
            latch.await();
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw new ClientException(e);
        } finally {
            IoUtils.safeClose(connection);
        }
        String body = reference.get().getAttachment(Http2Client.RESPONSE_BODY);
        System.out.println("body = " +  body);;
        Optional<HeaderValues> contentTypeName = Optional.ofNullable(reference.get().getResponseHeaders().get(Headers.CONTENT_TYPE));
        SchemaValidatorsConfig config = new SchemaValidatorsConfig();
        ResponseValidator responseValidator = new ResponseValidator(config);
        int statusCode = reference.get().getResponseCode();
        System.out.println("statusCode = " + statusCode);
        Status status;
        if(contentTypeName.isPresent()) {
            status = responseValidator.validateResponseContent(body, requestUri, httpMethod, String.valueOf(statusCode), contentTypeName.get().getFirst());
        } else {
            status = responseValidator.validateResponseContent(body, requestUri, httpMethod, String.valueOf(statusCode), JSON_MEDIA_TYPE);
        }
        Assert.assertNull(status);
    }
}

