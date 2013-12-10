package testing;

import client.KVStore;
import common.messages.KVMessage;
import static junit.framework.Assert.assertTrue;
import org.junit.Test;

import junit.framework.TestCase;

public class AdditionalTest extends TestCase {
    
    private KVStore kvClient;
	
    @Override
    public void setUp() {
            kvClient = new KVStore("localhost", 50000);
            try {
                    kvClient.connect();
            } catch (Exception e) {
            }
    }
    @Override
    public void tearDown() {
            kvClient.disconnect();
    }

    @Test
    public void testNullKey() {
        String key = null;
        String value = "testNullKey";
        Exception ex = null;

        try {
                kvClient.put(key, value);
        } catch (Exception e) {
                ex = e;
        }

        assertNotNull(ex);
    }
    
    @Test
    public void testDeleteNonExistent() {
        String key = "testDeleteNonExistent";
        String value = null;
        KVMessage response = null;
        Exception ex = null;

        try {
                response = kvClient.put(key, value);
        } catch (Exception e) {
                ex = e;
        }

        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.DELETE_ERROR);
    }
    
    @Test
    public void testKeyTooLong() {
        String key = "12345678901234567890abcdef";
        String value = "this key is too long";
        KVMessage response = null;
        Exception ex = null;

        try {
                response = kvClient.put(key, value);
        } catch (Exception e) {
                ex = e;
        }

        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.PUT_ERROR);
    }
}
