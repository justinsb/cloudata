package com.cloudata.structured.sql.value;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

public class ValueHolder {
    ByteBuffer data;
    ValueCodec codec;
    long longValue;
    String stringValue;
    double doubleValue;
    boolean booleanValue;

    public void set(ByteBuffer data, ValueCodec codec) {
        this.data = data;
        this.codec = codec;
    }

    public void copyFrom(ValueHolder r) {
        this.data = r.data;
        this.longValue = r.longValue;
        this.doubleValue = r.doubleValue;
        this.stringValue = r.stringValue;
        this.booleanValue = r.booleanValue;
        this.codec = r.codec;
    }

    public void setNull() {
        this.data = null;
        this.stringValue = null;

        this.codec = ValueCodec.NULL;
    }

    public void set(long n) {
        this.data = null;
        this.stringValue = null;

        this.longValue = n;
        this.codec = ValueCodec.LONG;
    }

    public void set(double n) {
        this.data = null;
        this.stringValue = null;

        this.doubleValue = n;
        this.codec = ValueCodec.DOUBLE;
    }

    public void set(String s) {
        this.data = null;
        // this.stringValue = null;

        this.stringValue = s;
        this.codec = ValueCodec.STRING;
    }

    public void set(boolean v) {
        this.data = null;
        this.stringValue = null;

        this.booleanValue = v;
        this.codec = ValueCodec.BOOLEAN;
    }

    public void serializeTo(CodedOutputStream os) throws IOException {
        codec.serializeTo(this, os);
    }

    public void deserialize(CodedInputStream cis) throws IOException {
        byte b = cis.readRawByte();

        ValueCodec codec = ValueCodec.find(b);

        if (codec == null) {
            throw new IOException();
        }

        codec.deserialize(this, b, cis);

    }

    public boolean isNull() {
        return this.codec == ValueCodec.NULL;
    }

    public String getAsString() {
        return this.codec.getAsString(this);
    }

    @Override
    public String toString() {
        return "ValueHolder [" + getAsString() + "]";
    }

}
