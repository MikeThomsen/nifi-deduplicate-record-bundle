package org.apache.nifi.processor.standard;

import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;

import java.io.IOException;
import java.io.OutputStream;

public class StringSerializer implements Serializer<String> {
    @Override
    public void serialize(String s, OutputStream outputStream) throws SerializationException, IOException {
        outputStream.write(s.getBytes());
    }
}
