package com.cloudata.files.fs;

import io.netty.buffer.ByteBuf;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.stream.ChunkedNioStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.files.FilesModel.ChunkData;
import com.cloudata.files.blobs.BlobCache;

public class SimpleFsFile implements FsFile {

    private static final Logger log = LoggerFactory.getLogger(SimpleFsFile.class);

    private final FsClient fsClient;
    private final FsPath fsPath;
    private final ChunkData chunkData;

    public SimpleFsFile(FsClient fsClient, FsPath fsPath, ChunkData chunkData) {
        this.fsClient = fsClient;
        this.fsPath = fsPath;
        this.chunkData = chunkData;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public long getLength() {
        return chunkData.getLength();
    }

    @Override
    public ChunkedInput<ByteBuf> open() throws IOException {
        final BlobCache.CacheFileHandle handle;
        try {
            handle = fsClient.findChunk(chunkData).get();
        } catch (IOException e) {
            throw new IOException("Error opening blob", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IOException("Error opening blob", e);
        } catch (ExecutionException e) {
            throw new IOException("Error opening blob", e);
        }

        if (handle == null) {
            log.error("Blob not found: {}", chunkData);
            throw new IOException("Blob not found");
        }

        final FileInputStream fis = new FileInputStream(handle.getFile());

        return new ChunkedNioStream(fis.getChannel()) {
            @Override
            public void close() throws Exception {
                super.close();
                fis.close();
            }
        };
    }

    @Override
    public ChunkedInput<ByteBuf> open(Long from, Long to) throws IOException {
        return new SliceByteInput(open(), from, to);
    }

}
