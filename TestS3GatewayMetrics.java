/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.hadoop.ozone.s3.metrics;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.s3.endpoint.*;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link S3GatewayMetrics}.
 */
public class TestS3GatewayMetrics {

  private String bucketName = OzoneConsts.BUCKET;
  private OzoneClient clientStub;
  private BucketEndpoint bucketEndpoint;
  private RootEndpoint rootEndpoint;
  private ObjectEndpoint keyEndpoint;
  private OzoneBucket bucket;
  private HttpHeaders headers;
  private static final String ACL_MARKER = "acl";
  public static final String CONTENT = "0123456789";
  private S3GatewayMetrics metrics;


  @Before
  public void setup() throws Exception {
    clientStub = new OzoneClientStub();
    clientStub.getObjectStore().createS3Bucket(bucketName);
    bucket = clientStub.getObjectStore().getS3Bucket(bucketName);

    bucketEndpoint = new BucketEndpoint();
    bucketEndpoint.setClient(clientStub);

    rootEndpoint = new RootEndpoint();
    rootEndpoint.setClient(clientStub);

    keyEndpoint = new ObjectEndpoint();
    keyEndpoint.setClient(clientStub);
    keyEndpoint.setOzoneConfiguration(new OzoneConfiguration());

    headers = Mockito.mock(HttpHeaders.class);
    metrics = bucketEndpoint.getMetrics();
  }

  @Test
  public void testHeadBucket() throws Exception {

    long oriMetric = metrics.getHeadBucketSuccess();

    bucketEndpoint.head(bucketName);

    long curMetric = metrics.getHeadBucketSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testListBucket() throws Exception {

    long oriMetric = metrics.getListS3BucketsSuccess();

    rootEndpoint.get().getEntity();

    long curMetric = metrics.getListS3BucketsSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testHeadObject() throws Exception {
    String value = RandomStringUtils.randomAlphanumeric(32);
//    OzoneOutputStream out = bucket.createKey("key1",
//        value.getBytes(UTF_8).length, ReplicationType.RATIS,
//        ReplicationFactor.ONE, new HashMap<>());
//    out.write(value.getBytes(UTF_8));
//    out.close();

    ReplicationConfig config =
        ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
            ReplicationFactor.ONE);
    bucket.createKey("key1", value.getBytes(UTF_8).length, config,
        new HashMap<>());

    long oriMetric = metrics.getHeadKeySuccess();

    keyEndpoint.head(bucketName, "key1");

    long curMetric = metrics.getHeadKeySuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testGetBucketEndpointMetric() throws Exception {
    long oriMetric = metrics.getGetBucketSuccess();

    clientStub = createClientWithKeys("file1");
    bucketEndpoint.setClient(clientStub);
    bucketEndpoint.get(bucketName, null,
        null, null, 1000, null,
        null, null, "random", null,
        null, null).getEntity();

    long curMetric = metrics.getGetBucketSuccess();
    assertEquals(1L, curMetric - oriMetric);


    oriMetric = metrics.getGetBucketFailure();

    try {
      // Searching for a bucket that does not exist
      bucketEndpoint.get("newBucket", null,
          null, null, 1000, null,
          null, null, "random", null,
          null, null);
      fail();
    } catch (OS3Exception e) {
    }

    curMetric = metrics.getGetBucketFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testCreateBucketEndpointMetric() throws Exception {

    long oriMetric = metrics.getCreateBucketSuccess();

    bucketEndpoint.put(bucketName, null,
        null, null);
    long curMetric = metrics.getCreateBucketSuccess();

    assertEquals(1L, curMetric - oriMetric);


    // Creating an error by trying to create a bucket that already exists
    oriMetric = metrics.getCreateBucketFailure();

    bucketEndpoint.put(bucketName, null, null, null);

    curMetric = metrics.getCreateBucketFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testDeleteBucketEndpointMetric() throws Exception {
    long oriMetric = metrics.getDeleteBucketSuccess();

    bucketEndpoint.delete(bucketName);

    long curMetric = metrics.getDeleteBucketSuccess();
    assertEquals(1L, curMetric - oriMetric);


    oriMetric = metrics.getDeleteBucketFailure();
    try {
      // Deleting a bucket that does not exist will result in delete failure
      bucketEndpoint.delete(bucketName);
      fail();
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(), ex.getCode());
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getErrorMessage(),
          ex.getErrorMessage());
    }

    curMetric = metrics.getDeleteBucketFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testGetAclEndpointMetric() throws Exception {
    long oriMetric = metrics.getGetAclSuccess();

    Response response =
        bucketEndpoint.get(bucketName, null, null,
            null, 0, null, null, null,
            null, null, "acl", null);
    long curMetric = metrics.getGetAclSuccess();
    assertEquals(HTTP_OK, response.getStatus());
    assertEquals(1L, curMetric - oriMetric);


    oriMetric = metrics.getGetAclFailure();
    try {
      // Failing the getACL endpoint by applying ACL on a non-Existent Bucket
      bucketEndpoint.get("random_bucket", null,
          null, null, 0, null, null,
          null, null, null, "acl", null);
      fail();
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(), ex.getCode());
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getErrorMessage(),
          ex.getErrorMessage());
    }
    curMetric = metrics.getGetAclFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testPutAclEndpointMetric() throws Exception {
    long oriMetric = metrics.getPutAclSuccess();

    clientStub.getObjectStore().createS3Bucket("b1");
    InputStream inputBody = TestBucketAcl.class.getClassLoader()
        .getResourceAsStream("userAccessControlList.xml");

    bucketEndpoint.put("b1", ACL_MARKER, headers, inputBody);

    long curMetric = metrics.getPutAclSuccess();
    assertEquals(1L, curMetric - oriMetric);

    // Failing the putACL endpoint by applying ACL on a non-Existent Bucket
    oriMetric = metrics.getPutAclFailure();

    try {
      bucketEndpoint.put("unknown_bucket", ACL_MARKER, headers, inputBody);
      fail();
    } catch (Exception ex) {
      ex.getMessage();
    }
    curMetric = metrics.getPutAclFailure();
    assertEquals(1L, curMetric - oriMetric);
  }


  @Test
  public void testCreateKeyEndpointMetric() throws Exception{
    long oriMetric = metrics.getcreateKeySuccess();
    // Create an input stream
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    keyEndpoint.setHeaders(headers);
    // Create the file
    Response response = keyEndpoint.put(bucketName, "key1", CONTENT
        .length(), 1, null, body);

    long curMetric = metrics.getcreateKeySuccess();
    assertEquals(1L, curMetric - oriMetric);


    oriMetric = metrics.getcreateKeyFaliure();
    // Create an input stream
    body = new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    keyEndpoint.setHeaders(headers);
    // Create the file in a bucket that does not exist
    try {
      response = keyEndpoint.put("unknownBucket", "key1", CONTENT
          .length(), 1, null, body);
      fail();
    } catch (OS3Exception e) {
    }
    curMetric = metrics.getcreateKeyFaliure();
    assertEquals(1L, curMetric - oriMetric);
  }

  public void testInitMultiPartUploadSuccessEndpointMetric() throws Exception{

  }


  private OzoneClient createClientWithKeys(String... keys) throws IOException {
    OzoneClient client = new OzoneClientStub();

    client.getObjectStore().createS3Bucket(bucketName);
    OzoneBucket bkt = client.getObjectStore().getS3Bucket(bucketName);
    for (String key : keys) {
      bkt.createKey(key, 0).close();
    }
    return client;
  }
}
