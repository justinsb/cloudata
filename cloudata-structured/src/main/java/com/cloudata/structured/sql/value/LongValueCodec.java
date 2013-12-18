package com.cloudata.structured.sql.value;

import java.io.IOException;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

public class LongValueCodec extends ValueCodec {
    public static byte CODE = 'I';

    @Override
    public void serializeTo(ValueHolder value, CodedOutputStream os) throws IOException {
        assert value.codec == this;
        os.writeRawByte(CODE);
        os.writeSInt64NoTag(value.longValue);
    }

    @Override
    public void deserialize(ValueHolder valueHolder, byte code, CodedInputStream cis) throws IOException {
        assert code == CODE;

        long v = cis.readSInt64();
        valueHolder.set(v);
    }

    @Override
    public String getAsString(ValueHolder value) {
        assert value.codec == this;

        return Long.toString(value.longValue);
    }
}
