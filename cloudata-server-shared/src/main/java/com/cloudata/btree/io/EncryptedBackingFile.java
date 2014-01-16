package com.cloudata.btree.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class EncryptedBackingFile implements BackingFile {
    private static final Logger log = LoggerFactory.getLogger(EncryptedBackingFile.class);

    final BackingFile inner;

    final byte[] keyHashBytes;
    final SecretKeySpec secretKeySpec;

    final CipherSpec cipherSpec;

    public static final int BLOCK_SIZE = 256;

    public EncryptedBackingFile(BackingFile inner, CipherSpec cipherSpec, byte[] keyBytes) {
        this.inner = inner;
        this.cipherSpec = cipherSpec;
        this.secretKeySpec = cipherSpec.buildSecretKeySpec(keyBytes);
        this.keyHashBytes = Hashing.sha1().hashBytes(keyBytes).asBytes();

        Preconditions.checkArgument((this.getBlockSize() % cipherSpec.getBlockSize()) == 0);
        Preconditions.checkArgument((this.getBlockSize() % inner.getBlockSize()) == 0);
    }

    @Override
    public void close() throws IOException {
        inner.close();
    }

    @Override
    public void sync() throws IOException {
        inner.sync();

    }

    @Override
    public long size() {
        return inner.size();
    }

    @Override
    public ListenableFuture<Void> write(ByteBuffer src, long position) {
        int size = src.remaining();
        assert size != 0;
        Preconditions.checkArgument((size % BLOCK_SIZE) == 0);

        byte[] iv = constructIV(position);
        IvParameterSpec ivSpec = new IvParameterSpec(iv);

        ByteBuffer ciphertext;
        Cipher cipher = null;
        try {
            cipher = cipherSpec.borrow(Cipher.ENCRYPT_MODE, secretKeySpec, ivSpec);

            ciphertext = ByteBuffer.allocate(src.remaining());
            int n = cipher.doFinal(src, ciphertext);
            if (n != size) {
                throw new IllegalStateException();
            }
            ciphertext.flip();
        } catch (GeneralSecurityException e) {
            throw Throwables.propagate(e);
        } finally {
            cipherSpec.release(cipher);
        }
        return inner.write(ciphertext, position);
    }

    @Override
    public ListenableFuture<ByteBuffer> read(ByteBuffer buffer, final long position) {
        assert buffer.remaining() != 0;
        assert buffer.position() == 0;

        return Futures.transform(inner.read(buffer, position), new Function<ByteBuffer, ByteBuffer>() {

            @Override
            public ByteBuffer apply(ByteBuffer buffer) {
                int size = buffer.remaining();
                Preconditions.checkArgument((size % BLOCK_SIZE) == 0);

                byte[] iv = constructIV(position);
                IvParameterSpec ivSpec = new IvParameterSpec(iv);

                Cipher cipher = null;
                try {
                    cipher = cipherSpec.borrow(Cipher.DECRYPT_MODE, secretKeySpec, ivSpec);
                    int n = cipher.doFinal(buffer.duplicate(), buffer);
                    if (n != size) {
                        throw new IllegalStateException();
                    }
                    buffer.flip();
                } catch (GeneralSecurityException e) {
                    throw Throwables.propagate(e);
                } finally {
                    cipherSpec.release(cipher);
                }

                return buffer;
            }
        });
    }

    private byte[] constructIV(long position) {
        Preconditions.checkArgument((position % BLOCK_SIZE) == 0);

        long s = position / BLOCK_SIZE;

        Hasher hasher = Hashing.murmur3_128().newHasher();
        hasher.putLong(s);
        hasher.putBytes(keyHashBytes);
        byte[] iv = hasher.hash().asBytes();
        return iv;
    }

    @Override
    public int getBlockSize() {
        return BLOCK_SIZE;
    }

    @Override
    public String getPathInfo() {
        return this.inner.getPathInfo();
    }

}
