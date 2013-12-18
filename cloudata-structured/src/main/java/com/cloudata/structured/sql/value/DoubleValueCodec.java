package com.cloudata.structured.sql.value;

import java.io.IOException;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

public class DoubleValueCodec extends ValueCodec {
    public static byte CODE = 'F';

    @Override
    public void serializeTo(ValueHolder value, CodedOutputStream os) throws IOException {
        assert value.codec == this;
        os.writeRawByte(CODE);
        os.writeDoubleNoTag(value.doubleValue);
    }

    @Override
    public void deserialize(ValueHolder valueHolder, byte code, CodedInputStream cis) throws IOException {
        assert code == CODE;

        double v = cis.readDouble();
        valueHolder.set(v);
    }

    @Override
    public String getAsString(ValueHolder value) {
        assert value.codec == this;

        return Double.toString(value.doubleValue);
    }
}
