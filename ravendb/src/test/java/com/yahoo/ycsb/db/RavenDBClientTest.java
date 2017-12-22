package com.yahoo.ycsb.db;

import com.yahoo.ycsb.Status;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class RavenDBClientTest {

  private RavenDBClient client;

  @Before
  public void setup() throws Exception {
    // tODO set up your client

    client = new RavenDBClient();
    client.init();
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

  @After
  public void cleanup() throws Exception {
    // TODO: clean up method

    client.cleanup();
  }


}
