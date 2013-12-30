package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;

public class ParameterData {
    final Map<String, String> parameters = Maps.newHashMap();

    private static int findNull(final ByteBuf buffer) {
        final int n = buffer.writerIndex();
        for (int i = buffer.readerIndex(); i < n; i++) {
            final byte b = buffer.getByte(i);
            if (b == 0) {
                return i;
            }
        }
        return -1;
    }

    public static ParameterData read(ByteBuf data) throws IOException {
        ParameterData parameters = new ParameterData();

        while (data.isReadable()) {
            int start = data.readerIndex();
            int end = findNull(data);
            if (end == -1) {
                throw new IOException();
            }

            byte[] line = new byte[end - start];
            data.readBytes(line);

            int equals = -1;
            for (int i = 0; i < line.length; i++) {
                if (line[i] == '=') {
                    equals = i;
                    break;
                }
            }

            if (equals == -1) {
                throw new IOException();
            }

            String key = new String(line, 0, equals, Charsets.UTF_8);
            String value = new String(line, equals + 1, line.length - equals, Charsets.UTF_8);

            parameters.parameters.put(key, value);
        }

        return parameters;
    }

    public int estimateSize() {
        int size = 0;
        for (Entry<String, String> entry : parameters.entrySet()) {
            // We assume 1 byte characters
            size += entry.getKey().length() + 2 + entry.getValue().length();
        }
        size += 1;
        return size;
    }

    public void write(ByteBuf buf) {
        for (Entry<String, String> entry : parameters.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            buf.writeBytes(key.getBytes(Charsets.UTF_8));
            buf.writeByte('=');
            buf.writeBytes(value.getBytes(Charsets.UTF_8));
            buf.writeByte(0);
        }
        // buf.writeByte(0);
    }

    public void put(String key, String value) {
        parameters.put(key, value);
    }

}
