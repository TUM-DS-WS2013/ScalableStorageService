package app_kvServer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Class responsible for storing the key-value data.
 * @author Danila Klimenko
 */
public class KVDataStorage {
    private static final int    MAX_KEY_LENGTH = 20;
    private static final int    MAX_VALUE_LENGTH = 120 * 1024;
    
    private final Map<String, String>       storage;
    private final ReentrantReadWriteLock    rw_lock;
    private final Lock                      read_lock;
    private final Lock                      write_lock;
    
    /**
     * Main parameterless constructor.
     */
    public KVDataStorage() {
        this.storage = new HashMap<String, String>();
        this.rw_lock = new ReentrantReadWriteLock();
        this.read_lock = this.rw_lock.readLock();
        this.write_lock = this.rw_lock.writeLock();
    }
    
    /**
     * Method implementing the 'put' command.
     * @param key The key
     * @param value The value to be associated with the key
     * @return The previous value associated with the given key, or null, if the
     *          key was not present in the key-value storage.
     * @throws IllegalArgumentException Thrown if key or value are illegal
     */
    public String put(String key, String value) throws IllegalArgumentException {
        // Verify arguments
        if (key == null || key.length() > MAX_KEY_LENGTH) {
            throw new IllegalArgumentException("Illegal key: '" + key + "'.");
        }
        if (value == null || value.length() > MAX_VALUE_LENGTH) {
            throw new IllegalArgumentException("Illegal value: '" + key + "'.");
        }
        
        // Put (key,value) pair into storage
        String prev_value = null;
        
        write_lock.lock();
        try {
            prev_value = storage.put(key, value);
        } finally {
            write_lock.unlock();
        }
        
        return prev_value;
    }
    
    /**
     * Method implementing the 'get' command.
     * @param key The key to look for
     * @return The value associated with the given key, or null, if the key was
     *          not present in the key-value storage.
     */
    public String get(String key) {
        if (key == null || key.length() > MAX_KEY_LENGTH) {
            return null;
        }
        
        String value = null;
        
        read_lock.lock();
        try {
            value = storage.get(key);
        } finally {
            read_lock.unlock();
        }
        
        return value;
    }
    
    /**
     * Method implementing the 'get' command.
     * @param key The key to look for
     * @return The value previously associated with the given key, or null, if
     *          the key was not present in the key-value storage.
     */
    public String delete(String key) {
        if (key == null || key.length() > MAX_KEY_LENGTH) {
            return null;
        }
        
        String deleted_value = null;
        
        write_lock.lock();
        try {
            deleted_value = storage.remove(key);
        } finally {
            write_lock.unlock();
        }
        
        return deleted_value;
    }
    
    /**
     * Returns the dump of all the contents of the key-value storage.
     * @return A string containing all stored key-value data
     */
    public String dump() {
        return storage.toString();
    }
}
