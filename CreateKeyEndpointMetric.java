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
