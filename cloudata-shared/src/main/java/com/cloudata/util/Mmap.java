package com.cloudata.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

public class Mmap {
    public static MappedByteBuffer mmapFile(File file, long size) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.setLength(size);
            FileChannel fc = raf.getChannel();

            long position = 0;
            MappedByteBuffer mmap = fc.map(MapMode.READ_WRITE, position, size);
            return mmap;
        }
    }
}
