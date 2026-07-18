package dev.by1337.sync.bd.table;

import java.util.Map;

public class K2VPair<K, V> implements Map.Entry<K, V> {
    public final K key;
    public final V value;

    public K2VPair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public K getKey() {
        return null;
    }

    @Override
    public V getValue() {
        return null;
    }

    @Override
    public V setValue(V value) {
        throw new UnsupportedOperationException();
    }
}
