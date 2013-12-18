package com.cloudata.structured.sql.value;

import java.io.IOException;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

public class StringValueCodec extends ValueCodec {
    public static byte CODE = 'S';

    @Override
    public void serializeTo(ValueHolder value, CodedOutputStream os) throws IOException {
        assert value.codec == this;
        os.writeRawByte(CODE);
        os.writeStringNoTag(value.stringValue);
    }

    @Override
    public void deserialize(ValueHolder valueHolder, byte code, CodedInputStream cis) throws IOException {
        assert code == CODE;

        String s = cis.readString();
        valueHolder.set(s);
    }

    @Override
    public String getAsString(ValueHolder value) {
        assert value.codec == this;

        return value.stringValue;
    }
}
