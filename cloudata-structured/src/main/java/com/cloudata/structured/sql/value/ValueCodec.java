package com.cloudata.structured.sql.value;

import java.io.IOException;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

public abstract class ValueCodec {

    public static final NullValueCodec NULL = new NullValueCodec();
    public static final LongValueCodec LONG = new LongValueCodec();
    public static final DoubleValueCodec DOUBLE = new DoubleValueCodec();
    public static final StringValueCodec STRING = new StringValueCodec();
    public static final BooleanValueCodec BOOLEAN = new BooleanValueCodec();

    public abstract void serializeTo(ValueHolder value, CodedOutputStream os) throws IOException;

    private static final ValueCodec[] CODEC_MAP;
    static {
        CODEC_MAP = new ValueCodec[128];
        CODEC_MAP[NullValueCodec.CODE] = NULL;
        CODEC_MAP[LongValueCodec.CODE] = LONG;
        CODEC_MAP[DoubleValueCodec.CODE] = DOUBLE;
        CODEC_MAP[StringValueCodec.CODE] = STRING;
        CODEC_MAP[BooleanValueCodec.CODE_FALSE] = BOOLEAN;
        CODEC_MAP[BooleanValueCodec.CODE_TRUE] = BOOLEAN;
    }

    public static ValueCodec find(byte b) {
        ValueCodec codec;
        try {
            codec = CODEC_MAP[b];
        } catch (ArrayIndexOutOfBoundsException e) {
            codec = null;
        }
        return codec;
    }

    public abstract void deserialize(ValueHolder valueHolder, byte code, CodedInputStream cis) throws IOException;

    public abstract String getAsString(ValueHolder valueHolder);
}
