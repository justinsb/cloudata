package com.cloudata.structured.sql.value;

import java.io.IOException;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

public class NullValueCodec extends ValueCodec {
    public static byte CODE = 'N';

    @Override
    public void serializeTo(ValueHolder value, CodedOutputStream os) throws IOException {
        assert value.codec == this;

        os.writeRawByte(CODE);
    }

    @Override
    public void deserialize(ValueHolder valueHolder, byte code, CodedInputStream cis) throws IOException {
        assert code == CODE;

        valueHolder.setNull();
    }

    @Override
    public String getAsString(ValueHolder value) {
        assert value.codec == this;

        return null;
    }
}
