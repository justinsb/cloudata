package com.cloudata.util;

import java.io.File;
import java.io.IOException;

public class Directory {

  public static void mkdirs(File dir) throws IOException {
    if (!dir.exists() && !dir.mkdirs()) {
      throw new IllegalStateException("Cannot create directory " + dir);
    }
  }

}
