package com.cloudata.keyvalue.redis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.IOException;
import java.util.List;

public class RedisRequestDecoder extends ByteToMessageDecoder {

    private byte[][] arguments;
    private int argumentsPos = 0;

    enum State {
        ARGUMENT_COUNT, ARGUMENT_LENGTH, ARGUMENT_DATA
    }

    State state = State.ARGUMENT_COUNT;
    private int argumentLength;

    /**
     * Returns the index in the buffer of the CRLF found. Returns -1 if no CRLF was found in the buffer.
     */
    private static int findCrLf(final ByteBuf buffer) {
        final int n = buffer.writerIndex();
        for (int i = buffer.readerIndex(); i < n; i++) {
            final byte b = buffer.getByte(i);
            if (b == '\r' && i < n - 1 && buffer.getByte(i + 1) == '\n') {
                return i;
            }
        }
        return -1;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        switch (state) {
        case ARGUMENT_COUNT: {
            int crlf = findCrLf(in);
            if (crlf == -1) {
                return;
            }

            byte b = in.getByte(in.readerIndex());
            if (b == '*') {
                in.skipBytes(1);
                long l = Codec.readLong(in);
                if (l > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException();
                }
                int numArgs = (int) l;
                if (numArgs < 0) {
                    throw new RedisException("Invalid size: " + numArgs);
                }

                readCrLf(in);

                this.arguments = new byte[numArgs][];
                this.argumentsPos = 0;
                state = State.ARGUMENT_LENGTH;
            } else {
                // Inline command
                byte[] command = in.readBytes(crlf - in.readerIndex()).array();
                readCrLf(in);

                out.add(new RedisRequest(command, true));
            }
            break;
        }

        case ARGUMENT_LENGTH: {
            int crlf = findCrLf(in);
            if (crlf == -1) {
                return;
            }

            byte b = in.readByte();
            if (b == '$') {
                long l = Codec.readLong(in);
                readCrLf(in);

                if (l > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException();
                }
                int length = (int) l;
                if (length < 0) {
                    throw new RedisException("Invalid length: " + length);
                }

                this.argumentLength = length;
                state = State.ARGUMENT_DATA;
            } else {
                throw new IOException("Expected $ character");
            }
            break;
        }

        case ARGUMENT_DATA: {
            if (in.readableBytes() < (argumentLength + 2)) {
                return;
            }

            byte[] data = in.readBytes(argumentLength).array();
            readCrLf(in);

            arguments[argumentsPos] = data;
            argumentsPos++;
            if (argumentsPos >= arguments.length) {
                out.add(new RedisRequest(arguments));
                arguments = null;
                state = State.ARGUMENT_COUNT;
            }
            break;
        }
        }
    }

    private void readCrLf(ByteBuf in) throws RedisException {
        byte cr = in.readByte();
        byte lf = in.readByte();
        if (cr != '\r' || lf != '\n') {
            throw new RedisException("Expected CRLF");
        }
    }
}
