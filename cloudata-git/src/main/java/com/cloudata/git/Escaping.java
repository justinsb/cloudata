package com.cloudata.git;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;

public class Escaping {
  public static String escape(String s) {
    String escaped;
    try {
      escaped = URLEncoder.encode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException();
    }
    escaped = escaped.replace('%', ':');
    return escaped;
  }

  public static String asBase64Url(ByteString bytes) {
    return BaseEncoding.base64Url().encode(bytes.toByteArray());
  }

  public static ByteString fromBase64Url(String base64) {
    return ByteString.copyFrom(BaseEncoding.base64Url().decode(base64));
  }

}
