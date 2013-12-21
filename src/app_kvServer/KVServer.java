package app_kvServer;

import common.messages.ControlMessage;
import common.messages.NetworkMessage;
import server.ClientConnection;
import server.KVDataStorage;
import common.parsers.ArgumentParser;
import common.topology.HashValue;
import common.topology.ServerAddress;
import common.topology.ServiceMetaData;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ProtocolException;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.ParseException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import server.DataTransferRequest;
import server.KeyValuePacket;

public class KVServer implements Runnable {
    private enum ServerState {UNINITIALIZED, STOPPED, RUNNING, LOCKED};
    
    private static final Logger logger = LogSetup.getLogger();
    private final int           port;
    
    private final KVDataStorage         data_storage;
    private final ServerSocket          server_socket;
    private final Set<ClientConnection> clients;
    private ServerAddress               server_address;
    private volatile boolean            online;
    private volatile ServerState        state;
    private ServiceMetaData             meta_data;
    private HashValue                   range_begin;
    private HashValue                   range_end;
    
    /**
     * Constructor taking port number as its only argument
     * @param port Port number
     * @throws IOException Thrown if server socket cannot be created
     */
    public KVServer(int port) throws IOException {
        this.port = port;
        this.online = false;
        
        this.server_socket = new ServerSocket(this.port);
        
        this.data_storage = new KVDataStorage();
        this.clients = new HashSet<ClientConnection>();
        
        logger.info("Server listening on port: " + this.server_socket.getLocalPort());
        this.online = true;
        this.state = ServerState.UNINITIALIZED;
        this.meta_data = null;
        
        this.server_address = null;
        this.range_begin = null;
        this.range_end = null;
    }
    
    /**
     * Override for run() method from Runnable interface
     */
    @Override
    public void run() {
        while (this.online) {
            try {
                Socket client = server_socket.accept();
                ClientConnection connection = new ClientConnection(client, this);
                new Thread(connection).start();
                synchronized (this.clients) {
                    this.clients.add(connection);
                }
                
                logger.info("New connection from " + client.getInetAddress().getHostName() +
                            " from port " + client.getPort() + ".");
            } catch (IOException e) {
                if (this.online) {
                    logger.error("Error! Unable to establish connection: " + e.getMessage());
                }
            }
        }
        logger.info("Server stopped.");
    }
    
    /**
     * Returns the key-value storage used by the server
     * @return Key-value map
     */
    public KVDataStorage getDataStorage() {
        return this.data_storage;
    }
    
    /**
     * A callback function triggered by a client thread prior to its termination
     * @param client The client connection which is about to close
     */
    public void clientTerminated(ClientConnection client) {
        synchronized (this.clients) {
            this.clients.remove(client);
        }
    }
    
    /**
     * Shuts down the server and frees all corresponding resources. Also terminates
     * all client connection which are still active.
     */
    public void shutDown() {
        this.online = false;
        
        synchronized (this.clients) {
            for (ClientConnection client : this.clients) {
                client.closeConnection();
            }
            this.clients.clear();
        }
        
        if (!this.server_socket.isClosed()) {
            try {
                this.server_socket.close();
            } catch (IOException e) {
                logger.error("Error! Unable to close server socket: " + e.getMessage());
            }
        }
    }
    
    public boolean isResponsibleForKey(String key) {
        return HashValue.hashKey(key).isInRange(this.range_begin, this.range_end);
    }
    
    public boolean isStopped() {
        return this.state == ServerState.STOPPED || this.state == ServerState.UNINITIALIZED;
    }
    
    public boolean isLocked() {
        return this.state == ServerState.LOCKED;
    }
    
    public ServiceMetaData getMetaData() {
        return this.meta_data;
    }
    
    public void initialize(ServerAddress server_address, ServiceMetaData meta_data) throws IllegalStateException {
        if (this.state != ServerState.UNINITIALIZED) {
            throw new IllegalStateException("Cannot initialize server: illegal switch from state '" + this.state + "'.");
        }
        this.server_address = server_address;
        this.meta_data = meta_data;
        this.state = ServerState.STOPPED;
        
        HashValue[] range = this.meta_data.getHashRangeForServer(this.server_address);
        this.range_begin = range[0];
        this.range_end = range[1];
        
        logger.info("Server successfully initialized as '" + this.server_address + "'.");
    }
    
    public void start() throws IllegalStateException {
        if (this.state != ServerState.STOPPED) {
            throw new IllegalStateException("Cannot start server: illegal switch from state '" + this.state + "'.");
        }
        this.state = ServerState.RUNNING;
    }
    
    public void stop() throws IllegalStateException {
        if (this.state != ServerState.RUNNING) {
            throw new IllegalStateException("Cannot stop server: illegal switch from state '" + this.state + "'.");
        }
        this.state = ServerState.STOPPED;
    }
    
    public void lockWrite() throws IllegalStateException {
        if (this.state != ServerState.RUNNING) {
            throw new IllegalStateException("Cannot lock write operations: illegal switch from state '" + this.state + "'.");
        }
        this.state = ServerState.LOCKED;
    }
    
    public void unlockWrite() throws IllegalStateException {
        if (this.state != ServerState.LOCKED) {
            throw new IllegalStateException("Cannot unlock write operations: illegal switch from state '" + this.state + "'.");
        }
        this.state = ServerState.RUNNING;
    }
    
    public void updateMetaData(ServiceMetaData meta_data) throws IllegalStateException {
        this.meta_data = meta_data;
        
        HashValue[] range = this.meta_data.getHashRangeForServer(this.server_address);
        this.range_begin = range[0];
        this.range_end = range[1];
    }
    
    public void moveData(DataTransferRequest dt_request) throws IllegalStateException, ProtocolException {
        if (this.state != ServerState.LOCKED) {
            throw new IllegalStateException("Cannot transfer data while not in a 'LOCKED' state. Current state: '" 
                    + this.state + "'.");
        }
        
        KeyValuePacket          full_packet = this.data_storage.getPacketForHashRange(dt_request.getRangeBegin(),
                                                                                        dt_request.getRangeEnd());
        List<KeyValuePacket>    packets = full_packet.splitOnMarshaledSizeLimit(NetworkMessage.MAX_MESSAGE_SIZE);
        
        Socket          target_server = null;
        InputStream     input = null;
        OutputStream    output = null;
        try {
            target_server = new Socket(dt_request.getTarget().getAddress(), dt_request.getTarget().getPort());
            input = target_server.getInputStream();
            output = target_server.getOutputStream();
            
            for (KeyValuePacket packet : packets) {
                ControlMessage  ctrlmsg = new ControlMessage(ControlMessage.ControlType.TRANSFER, packet);
                NetworkMessage  netmsg = new NetworkMessage(ControlMessage.marshal(ctrlmsg));

                netmsg.writeTo(output);
                netmsg = NetworkMessage.readFrom(input);

                ctrlmsg = ControlMessage.unmarshal(netmsg.getData());
                if (ctrlmsg.getType() == ControlMessage.ControlType.FAILURE) {
                    throw new ProtocolException("Remote server (" + dt_request.getTarget().toString() + ") failed: " +
                            ctrlmsg.getDescription());
                } else if (ctrlmsg.getType() != ControlMessage.ControlType.SUCCESS) {
                    throw new ProtocolException("Remote server (" + dt_request.getTarget().toString() +
                            ") returned unexpected message: " + ctrlmsg.getType().name());
                }
            }
            
        } catch (ProtocolException ex) {
            throw ex;
        }catch (ParseException ex) {
            throw new ProtocolException("Failed to parse message from the remote server (" +
                    dt_request.getTarget().toString() + "): " + ex.getMessage());
        } catch (IOException ex) {
            throw new ProtocolException("Communication with remote server (" +
                    dt_request.getTarget().toString() + ") failed: " + ex.getMessage());
        } finally {
            try {
                if (input != null) {
                    input.close();
                }
                if (output != null) {
                    output.close();
                }
                if ((target_server != null) && (!target_server.isClosed())) {
                    target_server.close();
                }
            } catch (IOException ex) {
                logger.warn("Warning! Unable to tear down connection to remote server (" +
                    dt_request.getTarget().toString() + "): " + ex.getMessage());
            }
        }
    }
    
    public void deleteData(DataTransferRequest dt_request) throws IllegalStateException {
        if (this.state != ServerState.LOCKED) {
            throw new IllegalStateException("Cannot delete data while not in a 'LOCKED' state. Current state: '" 
                    + this.state + "'.");
        }
        this.data_storage.deleteHashRange(dt_request.getRangeBegin(), dt_request.getRangeEnd());
    }
    
    public void acceptTransferredData(KeyValuePacket packet) throws IllegalStateException {
        if (this.state != ServerState.RUNNING) {
            throw new IllegalStateException("Cannot receive data while not in a 'RUNNING' state. Current state: '" 
                    + this.state + "'.");
        }
        this.data_storage.putAllFromKeyValuePacket(packet);
    }
    
    public String getAddressAsString() {
        return (this.server_address != null) ? this.server_address.toString() : "?.?.?.?:" + this.port;
    }
    
    /**
     * The server's main() method.
     * @param args Array of command line arguments
     */
    public static void main(String[] args) {
        Level   log_level = Level.WARN;
        Integer port = null;
        
        // Parse command line arguments
        try {
            ArgumentParser parser = new ArgumentParser("hl:", args);
            ArgumentParser.Option option;
            
            while ((option = parser.getNextArgument()) != null) {
                if (option.name == null) { // Positional arguments go here
                    if (port == null) {
                        try {
                            port = Integer.parseInt(option.argument);
                        } catch (NumberFormatException e) {}
                        if (port == null || port < 0 || port > 65535) {
                            throw new ParseException("Invalid port number: " + option.argument + ".", 0);
                        }
                    } else {
                        throw new ParseException("Excess positional argument: " + option.argument + ".", 0);
                    }
                    
                } else if (option.name.equals("h")) {
                    System.exit(1);
                    
                } else if (option.name.equals("l")) {
                    if (LogSetup.isValidLevel(option.argument)) {
                        log_level = Level.toLevel(option.argument);
                    } else {
                        throw new ParseException("Invalid logging level: " + option.argument + ".", 0);
                    }
                }
            }
            
            if (port == null) {
                throw new ParseException("Port number is not provided.", 0);
            }
            
        } catch (ParseException e) {
            System.out.println("Error parsing command line arguments: " + e.getMessage());
            System.exit(1);
        }
        
        // Initialize logger
        try {
            File    f = new File(System.getProperty("java.class.path"));
            File    dir = f.getAbsoluteFile().getParentFile();
            String  path = dir.toString();
            LogSetup.initialize(path + "/logs/server/server.log", log_level);
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger: " + e.getMessage());
            System.exit(1);
        }
        
        // Start server
        try {
            KVServer server = new KVServer(port);
            server.run();
            
        } catch (IOException e) {
            logger.error("Error! Cannot start server: " + e.getMessage());
        }
    }
}
