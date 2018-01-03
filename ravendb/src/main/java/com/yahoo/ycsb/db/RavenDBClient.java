/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package com.yahoo.ycsb.db;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yahoo.ycsb.*;
import net.ravendb.client.Constants;
import net.ravendb.client.documents.DocumentStore;
import net.ravendb.client.documents.commands.*;
import net.ravendb.client.documents.commands.batches.BatchCommand;
import net.ravendb.client.documents.commands.batches.ICommandData;
import net.ravendb.client.documents.commands.batches.PutCommandDataWithJson;
import net.ravendb.client.documents.queries.IndexQuery;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * YCSB binding for RavenDB.
 */
public class RavenDBClient extends DB {

  public static final String RAVENDB_URL = "ravendb.url";
  public static final String RAVENDB_DATABASE = "ravendb.database";
  public static final String RAVENDB_CERTIFICATE = "ravendb.certificate";

  /** The batch size to use for inserts. */
  private static int batchSize;

  private static DocumentStore store;
  private static ObjectMapper mapper;

  private final List<ICommandData> bulkInserts = new ArrayList<ICommandData>();

  public static DocumentStore getStore() {
    return store;
  }

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();
    synchronized (RavenDBClient.class) {
      if (store != null) {
        return;
      }

      Properties props = getProperties();

      String urls = props.getProperty(RAVENDB_URL, null);
      if (urls == null) {
        urls = "http://localhost:8080";
      }

      String databaseName = props.getProperty(RAVENDB_DATABASE, "ycsb");
      String certificatePath = props.getProperty(RAVENDB_CERTIFICATE, null);
      batchSize = Integer.parseInt(props.getProperty("batchsize", "1"));

      try {
        DocumentStore documentStore = new DocumentStore(urls.split(","), databaseName);

        if (certificatePath != null) {
          KeyStore clientStore = KeyStore.getInstance("PKCS12");
          clientStore.load(new FileInputStream(certificatePath), "".toCharArray());
          documentStore.setCertificate(clientStore);
        }

        documentStore.initialize();

        System.out.println("ravendb client connection created with " + urls);
        store = documentStore;
        mapper = store.getConventions().getEntityMapper();
      } catch (Exception e1) {
        System.err
            .println("Could not initialize RavenDB store:"
                + e1.toString());
        e1.printStackTrace();
        return;
      }
    }
  }

  @Override
  public void cleanup() throws DBException {
    if (!bulkInserts.isEmpty()) {
      BatchCommand batchCommand = new BatchCommand(store.getConventions(), bulkInserts);
      store.getRequestExecutor().execute(batchCommand);

      bulkInserts.clear();
    }

    if (INIT_COUNT.decrementAndGet() == 0) {
      try {
        store.close();
      } catch (Exception e1) {
        System.err.println("Could not close RavenDB document store: "
            + e1.toString());
        e1.printStackTrace();
        return;
      } finally {
        store = null;
      }
    }
  }

  private String documentId(String table, String key) {
    return table + "/" + key;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    try {
      String documentId = documentId(table, key);

      GetDocumentsCommand command = new GetDocumentsCommand(documentId, null, false);
      store.getRequestExecutor().execute(command);
      GetDocumentsResult commandResult = command.getResult();

      if (commandResult != null && commandResult.getResults().size() > 0) {
        JsonNode json = commandResult.getResults().get(0);
        fillMap(json, result, fields);
        return Status.OK;
      } else {
        return Status.NOT_FOUND;
      }
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  private void fillMap(JsonNode json, HashMap<String, ByteIterator> result, Set<String> fields) {
    boolean checkFields = fields != null && !fields.isEmpty();
    for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.fields();
         jsonFields.hasNext();
      /* increment in loop body */) {

      Map.Entry<String, JsonNode> jsonField = jsonFields.next();
      String name = jsonField.getKey();
      if ((checkFields && !fields.contains(name)) || Constants.Documents.Metadata.KEY.equals(name)) {
        continue;
      }
      JsonNode jsonValue = jsonField.getValue();
      if (jsonValue != null && !jsonValue.isNull()) {
        result.put(name, new StringByteIterator(jsonValue.asText()));
      }
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    StringBuilder queryBuilder = new StringBuilder();

    try {
      String documentId = documentId(table, startkey);

      queryBuilder.append("from '")
          .append(table)
          .append("' where id() >= '")
          .append(documentId)
          .append("'");

      IndexQuery indexQuery = new IndexQuery(queryBuilder.toString());
      indexQuery.setPageSize(recordcount);
      indexQuery.setWaitForNonStaleResults(true);
      QueryCommand queryCommand = new QueryCommand(store.getConventions(), indexQuery, false, false);

      store.getRequestExecutor().execute(queryCommand);

      ArrayNode results = queryCommand.getResult().getResults();
      if (results.size() == 0) {
        return Status.ERROR;
      }

      result.ensureCapacity(recordcount);

      for (int i = 0; i < results.size(); i++) {
        JsonNode json = results.get(i);
        HashMap<String, ByteIterator> resultMap =
            new HashMap<String, ByteIterator>();

        fillMap(json, resultMap, fields);
        result.add(resultMap);
      }
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    return insertSingle(table, key, values);
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    if (batchSize == 1){
      return insertSingle(table, key, values);
    }
    return insertUsingBatch(table, key, values);
  }

  private Status insertSingle(String table, String key, HashMap<String, ByteIterator> values) {
    String documentId = documentId(table, key);

    try {
      ObjectNode node = toJson(values, table);

      PutDocumentCommand command = new PutDocumentCommand(documentId, null, node);
      store.getRequestExecutor().execute(command);

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status insertUsingBatch(String table, String key, HashMap<String, ByteIterator> values) {
    try {
      String documentId = documentId(table, key);
      ObjectNode document = toJson(values, table);

      ICommandData command = new PutCommandDataWithJson(documentId, null, document);
      bulkInserts.add(command);

      if (bulkInserts.size() == batchSize) {

        BatchCommand batchCommand = new BatchCommand(store.getConventions(), bulkInserts);
        store.getRequestExecutor().execute(batchCommand);

        bulkInserts.clear();
        return Status.OK;
      } else {
        return Status.BATCHED_OK;
      }
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    String documentId = documentId(table, key);

    try {
      DeleteDocumentCommand command = new DeleteDocumentCommand(documentId);
      store.getRequestExecutor().execute(command);
      return Status.OK;

    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  protected static ObjectNode toJson(Map<String, ByteIterator> values, String collection)
      throws IOException {
    ObjectNode node = mapper.createObjectNode();
    HashMap<String, String> stringMap = StringByteIterator.getStringMap(values);
    for (Map.Entry<String, String> pair : stringMap.entrySet()) {
      node.put(pair.getKey(), pair.getValue());
    }

    ObjectNode metadata = mapper.createObjectNode();
    metadata.set(Constants.Documents.Metadata.COLLECTION, mapper.valueToTree(collection));
    node.set(Constants.Documents.Metadata.KEY, metadata);

    return node;
  }
}
