package com.ltagliamonte.cassandra.audit;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Map;

/**
 * Decorator around the real query handler that audit all the performed queries.
 */
public class AuditQueryHandler implements QueryHandler {

    private static final QueryHandler realQueryHandler = QueryProcessor.instance;
    private static final Logger LOGGER = LoggerFactory.getLogger(AuditQueryHandler.class);
    private static String esIndexName = "cassandra_audit";
    private static String esHost = "127.0.0.1";
    private static Integer esPort = 443;
    private static String esSchema = "https";
    private static RestClient restClient;

    public AuditQueryHandler() {
        initVar();
        initESClient();
    }

    @Override
    public ResultMessage process(String query, QueryState queryState, QueryOptions queryOptions, Map<String, ByteBuffer> customPayload) throws RequestExecutionException, RequestValidationException {
        ClientState cs = queryState.getClientState();
        InetAddress clientAddress = queryState.getClientAddress();
        AuthenticatedUser user = cs.getUser();
        saveLogToElasticsearch(user.getName(), clientAddress.getHostAddress(), query);
        LOGGER.trace("AuditLog: {} at {} executed query {}", user.getName(), clientAddress.getHostAddress(), query);
        return realQueryHandler.process(query, queryState, queryOptions, customPayload);
    }

    @Override
    public ResultMessage.Prepared prepare(String query, QueryState queryState, Map<String, ByteBuffer> customPayload) throws RequestValidationException {
        return realQueryHandler.prepare(query, queryState, customPayload);
    }

    @Override
    public ParsedStatement.Prepared getPrepared(MD5Digest md5Digest) {
        return realQueryHandler.getPrepared(md5Digest);
    }

    @Override
    public ParsedStatement.Prepared getPreparedForThrift(Integer integer) {
        return realQueryHandler.getPreparedForThrift(integer);
    }

    @Override
    public ResultMessage processPrepared(CQLStatement cqlStatement, QueryState queryState, QueryOptions queryOptions, Map<String, ByteBuffer> customPayload) throws RequestExecutionException, RequestValidationException {
        ClientState cs = queryState.getClientState();
        InetAddress clientAddress = queryState.getClientAddress();
        AuthenticatedUser user = cs.getUser();
        saveLogToElasticsearch(user.getName(), clientAddress.getHostAddress(), cqlStatement.toString());
        LOGGER.trace("AuditLog: {} at {} executed query {}", user.getName(), clientAddress.getHostAddress(), cqlStatement.toString());
        return realQueryHandler.processPrepared(cqlStatement, queryState, queryOptions, customPayload);
    }

    @Override
    public ResultMessage processBatch(BatchStatement batchStatement, QueryState queryState, BatchQueryOptions batchQueryOptions, Map<String, ByteBuffer> customPayload) throws RequestExecutionException, RequestValidationException {
        ClientState cs = queryState.getClientState();
        InetAddress clientAddress = queryState.getClientAddress();
        AuthenticatedUser user = cs.getUser();
        saveLogToElasticsearch(user.getName(), clientAddress.getHostAddress(), batchStatement.toString());
        LOGGER.trace("AuditLog: {} at {} executed query {}", user.getName(), clientAddress.getHostAddress(), batchStatement.toString());
        return realQueryHandler.processBatch(batchStatement, queryState, batchQueryOptions, customPayload);
    }

    private void initVar() {
        Map<String, String> environment = System.getenv();
        if (!environment.isEmpty()) {
            if (environment.containsKey("CASSANDRA_AUDIT_INDEX_NAME")) {
                esIndexName = environment.get("CASSANDRA_AUDIT_INDEX_NAME");
                LOGGER.debug("Using {} as per CASSANDRA_AUDIT_INDEX_NAME", esIndexName);
            } else {
                LOGGER.debug("No value found for CASSANDRA_AUDIT_INDEX_NAME using default {}", esIndexName);
            }
            if (environment.containsKey("CASSANDRA_AUDIT_ES_ADDRESS")) {
                esHost = environment.get("CASSANDRA_AUDIT_ES_ADDRESS");
                LOGGER.debug("Using {} as per CASSANDRA_AUDIT_ES_ADDRESS", esHost);
            } else {
                LOGGER.debug("No value found for CASSANDRA_AUDIT_ES_ADDRESS using default {}", esHost);
            }
            if (environment.containsKey("CASSANDRA_AUDIT_ES_PORT")) {
                esPort = Integer.valueOf(environment.get("CASSANDRA_AUDIT_ES_PORT"));
                LOGGER.debug("Using {} as per CASSANDRA_AUDIT_ES_PORT", esPort);
            } else {
                LOGGER.debug("No value found for CASSANDRA_AUDIT_ES_PORT using default {}", esPort);
            }
            if (environment.containsKey("CASSANDRA_AUDIT_ES_SCHEMA")) {
                esSchema = environment.get("CASSANDRA_AUDIT_ES_SCHEMA");
                LOGGER.debug("Using {} as per CASSANDRA_AUDIT_ES_SCHEMA", esSchema);
            } else {
                LOGGER.debug("No value found for CASSANDRA_AUDIT_ES_SCHEMA using default {}", esSchema);
            }
        }
    }

    private void initESClient() {
        restClient = RestClient.builder(new HttpHost(esHost, esPort, esSchema))
                .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                    @Override
                    public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                        return requestConfigBuilder.setConnectTimeout(60000)
                                .setSocketTimeout(60000).setConnectionRequestTimeout(0);
                    }
                })
                .setMaxRetryTimeoutMillis(60000).build();
    }

    private void saveLogToElasticsearch(String user, String clientAddress, String query) {

        JSONObject payload = new JSONObject();
        payload.put("timestamp", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mmX").withZone(ZoneOffset.UTC).format(Instant.now()));
        payload.put("user", user);
        payload.put("client_address", clientAddress);
        payload.put("query", query);

        HttpEntity entity = new NStringEntity(payload.toString(), ContentType.APPLICATION_JSON);
        ResponseListener responseListener = new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                LOGGER.trace("Query Successfully indexed {}", response.toString());
            }

            @Override
            public void onFailure(Exception exception) {
                LOGGER.info("Failed to index query {}", exception.getStackTrace());
            }
        };
        restClient.performRequestAsync("POST", "/" + esIndexName + "/log/", Collections.<String, String>emptyMap(), entity, responseListener);
    }
}