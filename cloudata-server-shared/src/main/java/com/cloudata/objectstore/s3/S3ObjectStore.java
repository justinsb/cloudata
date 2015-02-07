package com.cloudata.objectstore.s3;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.cloudata.objectstore.ObjectInfo;
import com.cloudata.objectstore.ObjectStore;
import com.cloudata.util.ByteBufferOutputStream;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;

public class S3ObjectStore implements ObjectStore {

  private static final Logger log = LoggerFactory.getLogger(S3ObjectStore.class);

  final String bucket;

  final AmazonS3Client s3Client;

  public S3ObjectStore(String bucket, AWSCredentialsProvider awsCredentials) {
    this.bucket = bucket.startsWith("/") ? bucket.substring(1) : bucket;
    if (Strings.isNullOrEmpty(this.bucket)) {
      throw new IllegalArgumentException();
    }
    this.s3Client = new AmazonS3Client(awsCredentials);
  }

  @Override
  public void delete(String path) throws IOException {
    log.debug("Delete file from {}/{}", bucket, path);

    DeleteObjectRequest request = new DeleteObjectRequest(bucket, path);
    s3Client.deleteObject(request);
  }

  @Override
  public List<ObjectInfo> listChildren(String path, boolean recurse) throws IOException {
    log.debug("List children at {}/{}", bucket, path);

    ListObjectsRequest request = new ListObjectsRequest();
    request.setBucketName(this.bucket);
    request.setPrefix(path);
    if (!recurse) {
      request.setDelimiter("/");
      if (!path.endsWith("/")) {
        request.setPrefix(path + "/");
      }
    }

    List<ObjectInfo> children = Lists.newArrayList();

    ObjectListing objectListing = s3Client.listObjects(request);
    while (true) {

      for (String commonPrefix : objectListing.getCommonPrefixes()) {
        children.add(new S3DirInfo(commonPrefix));
      }

      for (S3ObjectSummary entry : objectListing.getObjectSummaries()) {
        children.add(new S3FileInfo(entry));
      }

      if (!objectListing.isTruncated()) {
        break;
      }

      objectListing = s3Client.listNextBatchOfObjects(objectListing);
    }

    return children;
  }

  @Override
  public ObjectInfo getInfo(final String path) throws IOException {
    log.debug("Get object info for {}/{}", bucket, path);

    GetObjectMetadataRequest request = new GetObjectMetadataRequest(bucket, path);
    final ObjectMetadata metadata = s3Client.getObjectMetadata(request);

    return new ObjectInfo() {

      @Override
      public long getLength() {
        return metadata.getContentLength();
      }

      @Override
      public String getPath() {
        return path;
      }

      @Override
      public boolean isSubdir() {
        return false;
      }

    };
  }

  @Override
  public void upload(String path, File src) throws IOException {
    log.debug("Upload file to {}/{}", bucket, path);

    PutObjectRequest request = new PutObjectRequest(bucket, path, src);
    s3Client.putObject(request);
  }

  @Override
  public void upload(String path, ByteString data) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long read(String path, ByteBuffer dst, long position) throws IOException {
    log.debug("Read file at {}/{}", bucket, path);

    GetObjectRequest request = new GetObjectRequest(bucket, path);
    request.setRange(position, position + dst.remaining() - 1);
    try (S3Object s3Object = s3Client.getObject(request)) {
      try (S3ObjectInputStream is = s3Object.getObjectContent()) {
        try (ByteBufferOutputStream os = new ByteBufferOutputStream(dst)) {
          return ByteStreams.copy(is, os);
        }
      }
    }
  }

  @Override
  public ByteString read(String path) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ObjectInfo read(String path, ByteSink sink) throws IOException {
    throw new UnsupportedOperationException();
  }

}
