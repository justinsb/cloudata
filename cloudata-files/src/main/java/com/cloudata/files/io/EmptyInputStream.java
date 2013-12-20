package com.cloudata.files.io;

import java.io.IOException;
import java.io.InputStream;

public class EmptyInputStream extends InputStream {

    @Override
    public int read() throws IOException {
        return -1;
    }

    @Override
    public int available() throws IOException {
        return 0;
    }

}
