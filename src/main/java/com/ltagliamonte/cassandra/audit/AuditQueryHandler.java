package com.ltagliamonte.cassandra.audit;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
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
import java.util.*;

/**
 * Decorator around the real query handler that audit all the performed queries.
 */
public class AuditQueryHandler implements QueryHandler {

    private static final QueryHandler realQueryHandler = QueryProcessor.instance;
    private static final Logger LOGGER = LoggerFactory.getLogger(AuditQueryHandler.class);
    private static ConcurrentLinkedHashMap<MD5Digest, String> preparedResult;
    private static ConcurrentLinkedHashMap<CQLStatement, MD5Digest> preparedStatement;
    private static String esIndexName = "cassandra_audit";
    private static String esHost = "127.0.0.1";
    private static Integer esPort = 443;
    private static String esSchema = "https";
    private static boolean esDailyIndex = false;
    private static RestClient restClient;

    public AuditQueryHandler() {
        long MAX_CACHE_PREPARED_MEMORY = Runtime.getRuntime().maxMemory() / 256L;
        preparedResult = new ConcurrentLinkedHashMap.Builder<MD5Digest, String>()
                .maximumWeightedCapacity(MAX_CACHE_PREPARED_MEMORY)
                .build();
        preparedStatement = new ConcurrentLinkedHashMap.Builder<CQLStatement, MD5Digest>()
                .maximumWeightedCapacity(MAX_CACHE_PREPARED_MEMORY)
                .build();
        initVar();
        initESClient();
    }

    @Override
    public ResultMessage process(String query, QueryState queryState, QueryOptions queryOptions, Map<String, ByteBuffer> customPayload) throws RequestExecutionException, RequestValidationException {
        ClientState cs = queryState.getClientState();
        InetAddress clientAddress = queryState.getClientAddress();
        AuthenticatedUser user = cs.getUser();
        saveLogToElasticsearch(user.getName(), clientAddress.getHostAddress(), query, QUERY_TYPE.QUERY, null);
        LOGGER.trace("AuditLog: {} at {} executed query {}", user.getName(), clientAddress.getHostAddress(), query);
        return realQueryHandler.process(query, queryState, queryOptions, customPayload);
    }

    @Override
    public ResultMessage.Prepared prepare(String query, QueryState queryState, Map<String, ByteBuffer> customPayload) throws RequestValidationException {
        ResultMessage.Prepared prepQuery = realQueryHandler.prepare(query, queryState, customPayload);
        if (null != prepQuery && null != query) {
            preparedResult.putIfAbsent(prepQuery.statementId, query);
        }
        return prepQuery;
    }

    @Override
    public ParsedStatement.Prepared getPrepared(MD5Digest md5Digest) {
        ParsedStatement.Prepared prepStat = realQueryHandler.getPrepared(md5Digest);
        if (null != prepStat && null != md5Digest) {
            preparedStatement.putIfAbsent(prepStat.statement, md5Digest);
        }
        return prepStat;
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
        String query = "";
        if (null != cqlStatement) {
            query = preparedResult.get(preparedStatement.get(cqlStatement));
        }
        saveLogToElasticsearch(user.getName(), clientAddress.getHostAddress(), query, QUERY_TYPE.PREPARED_STATEMENT, null);
        LOGGER.trace("AuditLog processPrepared: {} at {} executed query {}", user.getName(), clientAddress.getHostAddress(), query);
        return realQueryHandler.processPrepared(cqlStatement, queryState, queryOptions, customPayload);
    }

    @Override
    public ResultMessage processBatch(BatchStatement batchStatement, QueryState queryState, BatchQueryOptions batchQueryOptions, Map<String, ByteBuffer> customPayload) throws RequestExecutionException, RequestValidationException {
        ClientState cs = queryState.getClientState();
        InetAddress clientAddress = queryState.getClientAddress();
        AuthenticatedUser user = cs.getUser();
        if (null != batchStatement.getStatements()) {
            UUID batchID = UUID.randomUUID();
            Set<ModificationStatement> querySet = new HashSet<ModificationStatement>(batchStatement.getStatements()); //If the batch request contains the same statement just log it one time
            Iterator<ModificationStatement> queryIter = querySet.iterator();
            while (queryIter.hasNext()) {
                String logQuery = preparedResult.get(preparedStatement.get(queryIter.next()));
                saveLogToElasticsearch(user.getName(), clientAddress.getHostAddress(), logQuery, QUERY_TYPE.BATCH, batchID);
                LOGGER.trace("AuditLog processBatch: {} at {} executed query {}", user.getName(), clientAddress.getHostAddress(), logQuery);
            }
        }
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
            if (environment.containsKey("CASSANDRA_AUDIT_DAILY_INDEX")) {
                esDailyIndex = Boolean.valueOf(environment.get("CASSANDRA_AUDIT_DAILY_INDEX"));
                LOGGER.debug("Using {} as per CASSANDRA_AUDIT_DAILY_INDEX", String.valueOf(esDailyIndex));
            } else {
                LOGGER.debug("No value found for CASSANDRA_AUDIT_DAILY_INDEX using default {}", String.valueOf(esDailyIndex));
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

    private void saveLogToElasticsearch(String user, String clientAddress, String query, QUERY_TYPE type, UUID batchID) {
        JSONObject payload = new JSONObject();
        payload.put("timestamp", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mmX").withZone(ZoneOffset.UTC).format(Instant.now()));
        payload.put("user", user);
        payload.put("client_address", clientAddress);
        payload.put("query_type", type.toString());
        payload.put("batch_id", String.valueOf(batchID));
        payload.put("query", query);

        HttpEntity entity = new NStringEntity(payload.toString(), ContentType.APPLICATION_JSON);
        ResponseListener responseListener = new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                LOGGER.trace("Query Successfully indexed {}", response.toString());
            }

            @Override
            public void onFailure(Exception exception) {
                LOGGER.error("Failed to index query {}", exception.getStackTrace());
            }
        };
        String indexName = esIndexName;
        if (esDailyIndex) {
            indexName = esIndexName + "-" + DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC).format(Instant.now());
        }
        restClient.performRequestAsync("POST", "/" + indexName + "/log/", Collections.<String, String>emptyMap(), entity, responseListener);
    }

    private enum QUERY_TYPE {
        QUERY, BATCH, PREPARED_STATEMENT
    }
}