package com.cloudata.btree.io;

import java.security.GeneralSecurityException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class CipherSpec {

    public static final CipherSpec AES_128 = new CipherSpec("AES/CBC/NoPadding");

    final ConcurrentLinkedQueue<Cipher> pool;
    final String cipherName;
    final String secretKeySpecName;
    final int blockSize;

    public CipherSpec(String cipherName) {
        this.pool = new ConcurrentLinkedQueue<Cipher>();

        this.cipherName = cipherName;
        if (cipherName.contains("/")) {
            this.secretKeySpecName = cipherName.substring(0, cipherName.indexOf('/'));
        } else {
            this.secretKeySpecName = cipherName;
        }

        Cipher cipher = null;
        try {
            cipher = borrow();
            this.blockSize = cipher.getBlockSize();
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException("Unable to build cipher", e);
        } finally {
            release(cipher);
        }

    }

    private Cipher borrow() throws NoSuchAlgorithmException, NoSuchPaddingException {
        Cipher cipher = pool.poll();
        if (cipher == null) {
            cipher = Cipher.getInstance(cipherName);
        }
        return cipher;
    }

    public SecretKeySpec buildSecretKeySpec(byte[] keyBytes) {
        return new SecretKeySpec(keyBytes, secretKeySpecName);
    }

    public Cipher borrow(int mode, SecretKeySpec secretKeySpec, IvParameterSpec ivSpec)
            throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException,
            InvalidAlgorithmParameterException {
        Cipher cipher = borrow();
        cipher.init(mode, secretKeySpec, ivSpec);
        return cipher;
    }

    public void release(Cipher cipher) {
        if (cipher != null) {
            pool.add(cipher);
        }
    }

    /**
     * Returns the block size of the cipher (in bytes)
     * 
     * @return
     */
    public int getBlockSize() {
        return blockSize;
    }
}
