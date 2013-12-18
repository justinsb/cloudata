package com.cloudata.structured.sql.value;

import java.io.IOException;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

public class BooleanValueCodec extends ValueCodec {
    public static byte CODE_TRUE = 't';
    public static byte CODE_FALSE = 'f';

    @Override
    public void serializeTo(ValueHolder value, CodedOutputStream os) throws IOException {
        assert value.codec == this;
        if (value.booleanValue) {
            os.writeRawByte(CODE_TRUE);
        } else {
            os.writeRawByte(CODE_FALSE);
        }
    }

    @Override
    public void deserialize(ValueHolder valueHolder, byte code, CodedInputStream cis) throws IOException {
        if (code == CODE_TRUE) {
            valueHolder.set(true);
        } else {
            assert code == CODE_FALSE;
            valueHolder.set(false);
        }
    }

    @Override
    public String getAsString(ValueHolder value) {
        assert value.codec == this;

        return Boolean.toString(value.booleanValue);
    }
}
