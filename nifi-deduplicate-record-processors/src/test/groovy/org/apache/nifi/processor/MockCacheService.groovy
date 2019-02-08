package org.apache.nifi.processor

import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.distributed.cache.client.Deserializer
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient
import org.apache.nifi.distributed.cache.client.Serializer

class MockCacheService extends AbstractControllerService implements DistributedMapCacheClient {
    @Override
    def <K, V> boolean putIfAbsent(K k, V v, Serializer<K> serializer, Serializer<V> serializer1) throws IOException {
        return false
    }

    @Override
    def <K, V> V getAndPutIfAbsent(K k, V v, Serializer<K> serializer, Serializer<V> serializer1, Deserializer<V> deserializer) throws IOException {
        return null
    }

    @Override
    def <K> boolean containsKey(K k, Serializer<K> serializer) throws IOException {
        return false
    }

    @Override
    def <K, V> void put(K k, V v, Serializer<K> serializer, Serializer<V> serializer1) throws IOException {

    }

    @Override
    def <K, V> V get(K k, Serializer<K> serializer, Deserializer<V> deserializer) throws IOException {
        return null
    }

    @Override
    void close() throws IOException {

    }

    @Override
    def <K> boolean remove(K k, Serializer<K> serializer) throws IOException {
        return false
    }

    @Override
    long removeByPattern(String s) throws IOException {
        return 0
    }
}
