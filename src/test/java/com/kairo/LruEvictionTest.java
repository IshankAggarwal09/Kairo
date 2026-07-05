package com.kairo;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class LruEvictionTest {

    @Test
    public void testLruEviction() {
        // Cache with max capacity of 3
        CacheStore store = new CacheStore(5, 3);
        
        // Insert 3 items
        store.put("key1", "val1");
        store.put("key2", "val2");
        store.put("key3", "val3");
        
        assertEquals("val1", store.get("key1"));
        assertEquals("val2", store.get("key2"));
        assertEquals("val3", store.get("key3"));
        
        // Access key1 to make it most recently used
        store.get("key1");
        
        // Insert key4, this should evict the least recently used, which is key2
        store.put("key4", "val4");
        
        assertNull(store.get("key2"), "key2 should have been evicted");
        assertEquals("val1", store.get("key1"), "key1 should still be present");
        assertEquals("val3", store.get("key3"), "key3 should still be present");
        assertEquals("val4", store.get("key4"), "key4 should still be present");
        
        // Insert key5, this should evict key1 because the order of gets above made key1 the LRU
        store.put("key5", "val5");
        
        assertNull(store.get("key1"), "key1 should have been evicted");
        assertEquals("val3", store.get("key3"), "key3 should still be present");
        assertEquals("val4", store.get("key4"), "key4 should still be present");
        assertEquals("val5", store.get("key5"), "key5 should still be present");
    }
}
