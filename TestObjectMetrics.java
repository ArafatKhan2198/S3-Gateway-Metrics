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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
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
import java.util.HashMap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Utils.urlEncode;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG =
      LoggerFactory.getLogger(ObjectEndpoint.class);


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
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");
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
  public void testHeadObjectSuccess() throws Exception {
    String value = RandomStringUtils.randomAlphanumeric(32);
    OzoneOutputStream out = bucket.createKey("key1",
        value.getBytes(UTF_8).length, ReplicationType.RATIS,
        ReplicationFactor.ONE, new HashMap<>());
    out.write(value.getBytes(UTF_8));
    out.close();

    // Test for Success of HeadKeySuccess Metric
    long oriMetric = metrics.getHeadKeySuccess();

    keyEndpoint.head(bucketName, "key1");

    long curMetric = metrics.getHeadKeySuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testHeadObjectFailure() throws Exception {
    // Test for Failure of HeadKeyFailure Metric
    long oriMetric = metrics.getHeadKeyFailure();

    keyEndpoint.head(bucketName, "unknownKey");

    long curMetric = metrics.getHeadKeyFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testCreateKeySuccess() throws Exception {

    // Test for Success of CreateKeySuccess Metric
    long oriMetric = metrics.getCreateKeySuccess();
    // Create an input stream
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    keyEndpoint.setHeaders(headers);
    // Create the file
    keyEndpoint.put(bucketName, "key1", CONTENT
        .length(), 1, null, body);
    body.close();
    long curMetric = metrics.getCreateKeySuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testCreateKeyFailure() throws Exception {
    // Test for Success of createKeyFailure Metric
    long oriMetric = metrics.getCreateKeyFailure();
    keyEndpoint.setHeaders(headers);
    // Create the file in a bucket that does not exist
    try {
      keyEndpoint.put("unknownBucket", "key1", CONTENT
          .length(), 1, null, null);
      fail();
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(), ex.getCode());
    }
    long curMetric = metrics.getCreateKeyFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testInitMultiPartUploadSuccess() throws Exception {
    keyEndpoint.setHeaders(headers);

    // Test for Success of InitMultiPartUploadSuccess Metric
    long oriMetric = metrics.getInitMultiPartUploadSuccess();
    keyEndpoint.initializeMultipartUpload(bucketName, "key1");
    long curMetric = metrics.getInitMultiPartUploadSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testInitMultiPartUploadFailure() throws Exception {
    // Test for Success of InitMultiPartUploadFailure Metric
    long oriMetric = metrics.getInitMultiPartUploadFailure();
    keyEndpoint.setHeaders(headers);
    try {
      keyEndpoint.initializeMultipartUpload("unknownBucket", "key1");
      fail();
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(), ex.getCode());
    }
    long curMetric = metrics.getInitMultiPartUploadFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testDeleteKeySuccess() throws Exception {
    // Test for Success of DeleteKeySuccess Metric
    long oriMetric = metrics.getDeleteKeySuccess();

    OzoneBucket bucket =
        clientStub.getObjectStore().getS3Bucket(bucketName);

    bucket.createKey("key1", 0).close();
    keyEndpoint.delete(bucketName, "key1", null);
    long curMetric = metrics.getDeleteKeySuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testDeleteKeyFailure() throws Exception {
    // Test for Success of DeleteKeyFailure Metric
    long oriMetric = metrics.getDeleteKeyFailure();
    try {
      keyEndpoint.delete("unknownBucket", "key1", null);
      fail();
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(), ex.getCode());
    }
    long curMetric = metrics.getDeleteKeyFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testGetKeySuccess() throws Exception {
    // Test for Success of GetKeySuccess Metric
    long oriMetric = metrics.getGetKeySuccess();

    // Create an input stream
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    keyEndpoint.setHeaders(headers);
    // Create the file
    keyEndpoint.put(bucketName, "key1", CONTENT
        .length(), 1, null, body);
    // GET the key from the bucket
    keyEndpoint.get(bucketName, "key1", null, 0,
        null, body);
    long curMetric = metrics.getGetKeySuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testGetKeyFailure() throws Exception {
    // Test for Success of GetKeyFailure Metric
    long oriMetric = metrics.getGetKeyFailure();
    keyEndpoint.setHeaders(headers);
    // Fetching a non-existent key
    try {
      keyEndpoint.get(bucketName, "unknownKey", null, 0,
          null, null);
      fail();
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_KEY.getCode(), ex.getCode());
    }
    long curMetric = metrics.getGetKeyFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testAbortMultiPartUploadSuccess() throws Exception {
    keyEndpoint.setHeaders(headers);

    // Initiate the Upload and fetch the upload ID
    String uploadID = initiateMultipartUpload(bucketName, "key1");

    // Test for Success of AbortMultiPartUploadSuccess Metric
    long oriMetric = metrics.getAbortMultiPartUploadSuccess();

    // Abort the Upload Successfully by deleting the key using the Upload-Id
    keyEndpoint.delete(bucketName, "key1", uploadID);

    long curMetric = metrics.getAbortMultiPartUploadSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testAbortMultiPartUploadFailure() throws Exception {
    // Test for Success of AbortMultiPartUploadFailure Metric
    long oriMetric = metrics.getAbortMultiPartUploadFailure();

    // Fail the Abort Method by providing wrong uploadID
    try {
      keyEndpoint.delete(bucketName, "key1", "wrongId");
      fail();
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_UPLOAD.getCode(), ex.getCode());
    }
    long curMetric = metrics.getAbortMultiPartUploadFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testCompleteMultiPartUploadSuccess() throws Exception {
    keyEndpoint.setHeaders(headers);

    // Initiate the Upload and fetch the upload ID
    String uploadID = initiateMultipartUpload(bucketName, "key1");

    // Test for Success of CompleteMultiPartUploadSuccess Metric
    long oriMetric = metrics.getCompleteMultiPartUploadSuccess();
    // complete multipart upload
    CompleteMultipartUploadRequest completeMultipartUploadRequest = new
        CompleteMultipartUploadRequest();
    Response response = keyEndpoint.completeMultipartUpload(bucketName, "key1",
        uploadID, completeMultipartUploadRequest);
    long curMetric = metrics.getCompleteMultiPartUploadSuccess();
    assertEquals(200, response.getStatus());
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testCompleteMultiPartUploadFailure() throws Exception {
    // Test for Success of CompleteMultiPartUploadFailure Metric
    long oriMetric = metrics.getCompleteMultiPartUploadFailure();
    CompleteMultipartUploadRequest completeMultipartUploadRequestNew = new
        CompleteMultipartUploadRequest();
    try {
      keyEndpoint.completeMultipartUpload(bucketName, "key2",
          "random", completeMultipartUploadRequestNew);
      fail();
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_UPLOAD.getCode(), ex.getCode());
    }
    long curMetric = metrics.getCompleteMultiPartUploadFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testCreateMultipartKeySuccess() throws Exception {
    keyEndpoint.setHeaders(headers);

    // Initiate the Upload and fetch the upload ID
    String uploadID = initiateMultipartUpload(bucketName, "key1");

    // Test for Success of CreateMultipartKeySuccess Metric
    long oriMetric = metrics.getCreateMultipartKeySuccess();
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    keyEndpoint.put(bucketName, "key1", CONTENT.length(),
        1, uploadID, body);
    long curMetric = metrics.getCreateMultipartKeySuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testCreateMultipartKeyFailure() throws Exception {
    keyEndpoint.setHeaders(headers);
    // Test for Success of CreateMultipartKeyFailure Metric
    long oriMetric = metrics.getCreateMultipartKeyFailure();
    try {
      keyEndpoint.put(bucketName, "key1", CONTENT.length(),
          1, "randomId", null);
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_UPLOAD.getCode(), ex.getCode());
    }
    long curMetric = metrics.getCreateMultipartKeyFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testListPartsSuccess() throws Exception {

    keyEndpoint.setHeaders(headers);

    // Test for Success of ListPartsSuccess Metric
    long oriMetric = metrics.getListPartsSuccess();
    // Initiate the Upload and fetch the upload ID
    String uploadID = initiateMultipartUpload(bucketName, "key1");

    // Listing out the parts by providing the uploadID
    keyEndpoint.get(bucketName, "key1",
        uploadID, 3, null, null);
    long curMetric = metrics.getListPartsSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testListPartsFailure() throws Exception {
    keyEndpoint.setHeaders(headers);

    // Test for Success of ListPartsFailure Metric
    long oriMetric = metrics.getListPartsFailure();
    try {
      // Listing out the parts by providing the uploadID after aborting the upload
      keyEndpoint.get(bucketName, "key1",
          "wrong_id", 3, null, null);
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_UPLOAD.getCode(), ex.getCode());
    }
    long curMetric = metrics.getListPartsFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

    @Test
  public void testCopyObjectSuccess() throws Exception {

    keyEndpoint.setHeaders(headers);

    String destBucket = "b2";
    String destKey = "key2";

    // Create bucket
    clientStub.getObjectStore().createS3Bucket(destBucket);


    // Test for Success of CopyObjectSuccess Metric
    long oriMetric = metrics.getCopyObjectSuccess();
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));

    keyEndpoint.put(bucketName, "key1",
        CONTENT.length(), 1, null, body);

    // Add copy header, and then call put
    when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
        bucketName + "/" + urlEncode("key1"));

    keyEndpoint.put(destBucket, destKey, CONTENT.length(), 1,
        null, body);
    long curMetric = metrics.getCopyObjectSuccess();
    assertEquals(1L, curMetric - oriMetric);

    // Test for Success of CopyObjectFailure Metric
    oriMetric = metrics.getCopyObjectFailure();
    // source and dest same
    try {
      when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn("");
      keyEndpoint.put(bucketName, "key1", CONTENT.length(), 1, null, body);
      fail("Test for CopyObjectMetric failed");
    } catch (OS3Exception ex) {
      Assert.assertTrue(ex.getErrorMessage().contains("This copy request is " +
          "illegal"));
    }
    curMetric = metrics.getCopyObjectFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

    private OzoneClient createClientWithKeys(String... keys) throws IOException {
    OzoneBucket bkt = clientStub.getObjectStore().getS3Bucket(bucketName);
    for (String key : keys) {
      bkt.createKey(key, 0).close();
    }
    return clientStub;
  }

  private String initiateMultipartUpload(String bucketName, String key)
      throws IOException,
      OS3Exception {
    // Initiate the Upload
    Response response =
        keyEndpoint.initializeMultipartUpload(bucketName, key);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    if (response.getStatus() == 200) {
      // Fetch the Upload-Id
      String uploadID = multipartUploadInitiateResponse.getUploadID();
      return uploadID;
    }
    return "Invalid-Id";
  }
}
