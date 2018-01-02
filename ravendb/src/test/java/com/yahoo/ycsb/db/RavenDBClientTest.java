package com.yahoo.ycsb.db;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import net.ravendb.client.Constants;
import net.ravendb.client.documents.DocumentStore;
import net.ravendb.client.documents.operations.DeleteByQueryOperation;
import net.ravendb.client.documents.queries.IndexQuery;
import net.ravendb.client.documents.queries.Query;
import net.ravendb.client.documents.session.IDocumentSession;
import org.junit.*;

import java.sql.SQLException;
import java.util.*;

import static org.junit.Assert.*;

public class RavenDBClientTest {

  private static final String TEST_RAVEN_URL = "http://localhost:8080";
  private static final String TEST_RAVEN_DATABASE = "test";
  private static final String TABLE_NAME = "USERTABLE";
  private static final int FIELD_LENGTH = 32;
  private static final String FIELD_PREFIX = "FIELD";
  private static final String KEY_PREFIX = "user";
  private static final String KEY_FIELD = "YCSB_KEY";
  private static RavenDBClient client;

  @BeforeClass
  public static void setup() throws Exception {
    client = new RavenDBClient();

    Properties p = new Properties();
    p.setProperty(RavenDBClient.RAVENDB_URL, TEST_RAVEN_URL);
    p.setProperty(RavenDBClient.RAVENDB_DATABASE, TEST_RAVEN_DATABASE);

    client.setProperties(p);
    client.init();
  }

  @Before
  public void prepareTest() {
    DocumentStore store = RavenDBClient.getStore();
    IndexQuery query = new IndexQuery("from @all_docs");
    DeleteByQueryOperation operation = new DeleteByQueryOperation(query);
    store.operations().sendAsync(operation).waitForCompletion();
  }

  @Test
  public void testRead() {
    //use client to test read

    Set<String> fields = new HashSet<>();
    fields.add("FirstName");
    fields.add("LastName");
    Status result = client.read("users", "users/1", fields, /* TODO: */ null);
    Assert.assertEquals(Status.OK, result);
  }

  @AfterClass
  public static void teardown() {
    if (client != null) {
      try {
        client.cleanup();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /*
        This is a copy of buildDeterministicValue() from core:com.yahoo.ycsb.workloads.CoreWorkload.java.
        That method is neither public nor static so we need a copy.
     */
  private String buildDeterministicValue(String key, String fieldkey) {
    int size = FIELD_LENGTH;
    StringBuilder sb = new StringBuilder(size);
    sb.append(key);
    sb.append(':');
    sb.append(fieldkey);
    while (sb.length() < size) {
      sb.append(':');
      sb.append(sb.toString().hashCode());
    }
    sb.setLength(size);

    return sb.toString();
  }

  private HashMap<String, ByteIterator> insertRow(String insertKey) {
    HashMap<String, ByteIterator> insertMap = new HashMap<String, ByteIterator>();
    for (int i = 0; i < 3; i++) {
      insertMap.put(FIELD_PREFIX + i, new StringByteIterator(buildDeterministicValue(insertKey, FIELD_PREFIX + i)));
    }
    client.insert(TABLE_NAME, insertKey, insertMap);

    return insertMap;
  }

  @Test
  public void insertTest() {
    String insertKey = "user0";
    HashMap<String, ByteIterator> insertMap = insertRow(insertKey);

    try (IDocumentSession session = RavenDBClient.getStore().openSession()) {
      List<ObjectNode> result = session.query(ObjectNode.class, Query.collection(TABLE_NAME))
          .waitForNonStaleResults()
          .toList();

      // Check we have a result Row
      assertEquals(1, result.size());
      // Check that all the columns have expected values
      assertEquals(TABLE_NAME + "/" + insertKey,
          result
              .get(0)
              .get(Constants.Documents.Metadata.KEY)
              .get(Constants.Documents.Metadata.ID)
              .textValue());
      for (int i = 0; i < 3; i++) {
        assertEquals(result.get(0).get(FIELD_PREFIX + i).textValue(), insertMap.get(FIELD_PREFIX + i).toString());
      }
    }
  }

  // TODO: test for update?

  @Test
  public void readTest() {
    String insertKey = "user0";
    HashMap<String, ByteIterator> insertMap = insertRow(insertKey);
    HashSet<String> readFields = new HashSet<String>();
    HashMap<String, ByteIterator> readResultMap = new HashMap<String, ByteIterator>();

    // Test reading a single field
    readFields.add("FIELD0");
    client.read(TABLE_NAME, insertKey, readFields, readResultMap);
    assertEquals("Assert that result has correct number of fields", readFields.size(), readResultMap.size());
    for (String field: readFields) {
      assertEquals("Assert " + field + " was read correctly", insertMap.get(field).toString(), readResultMap.get(field).toString());
    }

    readResultMap = new HashMap<String, ByteIterator>();

    // Test reading all fields
    readFields.add("FIELD1");
    readFields.add("FIELD2");
    client.read(TABLE_NAME, insertKey, readFields, readResultMap);
    assertEquals("Assert that result has correct number of fields", readFields.size(), readResultMap.size());
    for (String field: readFields) {
      assertEquals("Assert " + field + " was read correctly", insertMap.get(field).toString(), readResultMap.get(field).toString());
    }
  }

  @Test
  public void deleteTest() {
    insertRow("user0");
    String deleteKey = "user1";
    insertRow(deleteKey);
    insertRow("user2");

    client.delete(TABLE_NAME, deleteKey);

    try (IDocumentSession session = RavenDBClient.getStore().openSession()) {
      List<ObjectNode> objectNodes = session.query(ObjectNode.class, Query.collection(TABLE_NAME))
          .toList();

      int totalRows = 0;
      for (ObjectNode objectNode : objectNodes) {
        String id = objectNode.get(Constants.Documents.Metadata.KEY)
            .get(Constants.Documents.Metadata.ID)
            .textValue();

        assertNotEquals("Assert this is not the deleted row key", deleteKey, id);
        totalRows++;
      }

      assertEquals("Assert we ended with the correct number of rows", totalRows, 2);
    }
  }

  @Test
  public void scanTest() throws SQLException {
    HashMap<String, HashMap<String, ByteIterator>> keyMap = new HashMap<String, HashMap<String, ByteIterator>>();
    for (int i = 0; i < 5; i++) {
      String insertKey = KEY_PREFIX + i;
      keyMap.put(insertKey, insertRow(insertKey));
    }
    HashSet<String> fieldSet = new HashSet<String>();
    fieldSet.add("FIELD0");
    fieldSet.add("FIELD1");
    int startIndex = 1;
    int resultRows = 3;

    Vector<HashMap<String, ByteIterator>> resultVector = new Vector<HashMap<String, ByteIterator>>();
    client.scan(TABLE_NAME, KEY_PREFIX + startIndex, resultRows, fieldSet, resultVector);

    // Check the resultVector is the correct size
    assertEquals("Assert the correct number of results rows were returned", resultRows, resultVector.size());
    // Check each vector row to make sure we have the correct fields
    int testIndex = startIndex;
    for (HashMap<String, ByteIterator> result: resultVector) {
      assertEquals("Assert that this row has the correct number of fields", fieldSet.size(), result.size());
      for (String field: fieldSet) {
        assertEquals("Assert this field is correct in this row", keyMap.get(KEY_PREFIX + testIndex).get(field).toString(), result.get(field).toString());
      }
      testIndex++;
    }
  }

  //TODO: batch tests

}
