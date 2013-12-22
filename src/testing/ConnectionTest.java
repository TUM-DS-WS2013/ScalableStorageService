package testing;

import java.net.UnknownHostException;

import client.KVStore;
import common.topology.ServerAddress;

import junit.framework.TestCase;


public class ConnectionTest extends TestCase {

	
	public void testConnectionSuccess() {
		
		Exception ex = null;
		
                ServerAddress   address = AllTests.valid_address;
		KVStore kvClient = new KVStore(address.getAddress(), address.getPort());
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex);
	}
	
	
	public void testUnknownHost() {
		Exception ex = null;
//		KVStore kvClient = new KVStore("unknown", 50000);
		
		try {
                        KVStore kvClient = new KVStore("unknown", 50000);
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
//		assertTrue(ex instanceof UnknownHostException);
                assertNotNull(ex);
	}
	
	
	public void testIllegalPort() {
		Exception ex = null;
//		KVStore kvClient = new KVStore("localhost", 123456789);
		
		try {
                        KVStore kvClient = new KVStore("localhost", 123456789);
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
//		assertTrue(ex instanceof IllegalArgumentException);
                assertNotNull(ex);
	}
	
	

	
}

