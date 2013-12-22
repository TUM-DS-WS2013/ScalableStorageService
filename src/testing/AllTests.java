package testing;

import app_kvEcs.ECSClient;
import app_kvEcs.ServiceNode;
import java.io.IOException;

import org.apache.log4j.Level;

import common.topology.ServerAddress;
import java.text.ParseException;
import java.util.List;
import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class AllTests {
    static ECSClient            ecsclient = null;
    public static ServerAddress valid_address = null;

    @BeforeClass
    public static void setUpClass() {
        try {
            LogSetup.initialize("logs/testing/test.log", Level.ALL);
            List<ServiceNode>   nodes = null;
            try {
                nodes = ServiceNode.parseServiceNodesFromFile("src/app_kvEcs/ecs.config");
            } catch (ParseException ex) {
                throw new IOException("Error parsing configurational file: " + ex.getMessage());
            }

            // Initialize and run ECSClient
            ecsclient = new ECSClient(nodes);
            new Thread(ecsclient).start();
            
            ecsclient.initializeNodes(4);
            ecsclient.startService();
            
            // Set a valid node address
            valid_address = ecsclient.test_getActiveNodeAddress();
            if (valid_address == null) {
                throw new IOException("Error: service has no active nodes.");
            }
            
        } catch (IOException ex) {
            try {
                if (ecsclient != null) {
                    ecsclient.shutDownService();
                }
            } catch (IOException ex2) {}
            
            System.err.println("Failed to initialize test suite: " + ex.getMessage());
            System.exit(1);
        }
    }
    
    @AfterClass
    public static void tearDownClass() throws IOException {
        if (ecsclient != null) {
            ecsclient.shutDownService();
        }
    }

    public static Test suite() {
        TestSuite clientSuite = new TestSuite("Scalable Storage Service Test-Suite");
        clientSuite.addTestSuite(ConnectionTest.class);
        clientSuite.addTestSuite(InteractionTest.class);
        clientSuite.addTestSuite(AdditionalTest.class);
//        return clientSuite;
        return new TestSetup(clientSuite) {
            protected void setUp() throws Exception {
                AllTests.setUpClass();
            }
            protected void tearDown() throws Exception {
                AllTests.tearDownClass();
            }
        };
    }

}
