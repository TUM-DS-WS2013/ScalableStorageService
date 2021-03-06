package client;

import common.messages.KVMessage;
import common.messages.KVMessageRaw;
import common.messages.NetworkMessage;
import common.topology.HashValue;
import common.topology.ServerAddress;
import common.topology.ServiceMetaData;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ProtocolException;
import java.net.Socket;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import logger.LogSetup;
import org.apache.log4j.Logger;

/**
 *
 * @author Danila Klimenko
 */
public class KVStore implements KVCommInterface {
    private static final Logger logger = LogSetup.getLogger();
    
    private final ServerAddress                         default_server_address;
    private final Map<ServerAddress, ServerConnection>  connections;
    private ServiceMetaData                             meta_data;
    private boolean                                     online;
    
    public KVStore(String address, int port) {
        this.default_server_address = new ServerAddress(address, port);
        this.connections = new HashMap<ServerAddress, ServerConnection>();
        this.meta_data = null;
        this.online = false;
    }

    @Override
    public void connect() throws Exception {
        this.addConnection(this.default_server_address);
        this.online = true;
    }

    @Override
    public void disconnect() {
        for (ServerConnection connecion : this.connections.values()) {
            connecion.disconnect();
        }
        this.connections.clear();
        this.online = false;
    }

    @Override
    public KVMessage put(String key, String value) throws Exception {
        if (!this.online) {
            throw new IOException("Operation 'put' is unavailable while client is offline.");
        }
        if (key == null) {
            throw new IllegalArgumentException("Key may not be null.");
        }
        return this.processRequest(new KVMessageRaw(KVMessage.StatusType.PUT, key, value));
    }

    @Override
    public KVMessage get(String key) throws Exception {
        if (!this.online) {
            throw new IOException("Operation 'get' is unavailable while client is offline.");
        }
        if (key == null) {
            throw new IllegalArgumentException("Key may not be null.");
        }
        return this.processRequest(new KVMessageRaw(KVMessage.StatusType.GET, key, null));
    }
    
    private ServerConnection addConnection(ServerAddress server_address) throws IOException {
        try {
            ServerConnection    connection = new ServerConnection(this, server_address);
            this.connections.put(server_address, connection);
            logger.info("Connected to a new server at '" + server_address + "'.");
            return connection;
            
        } catch (IOException ex) {
            logger.error("Failed to connect to server '" + server_address + "': " + ex.getMessage());
            throw ex;
        }
    }
    
    private ServerConnection getConnection(ServerAddress server_address) {
        return this.connections.get(server_address);
    }
    
    private ServerConnection getAnyConnection() {
        return this.connections.isEmpty() ? null : this.connections.values().iterator().next();
    }
    
    private ServerConnection findConnectionForKey(String key) {
        ServerConnection    connection;
        
        try {
            if (this.meta_data == null) {
                connection = this.getAnyConnection();
                if (connection == null) {
                    connection = this.addConnection(this.default_server_address);
                }
                
            } else {
                ServerAddress   address = this.meta_data.getServerForKey(key);

                connection = (this.hasConnection(address)) ? this.getConnection(address) : this.addConnection(address);
            }
            
        } catch (IOException ex) { // Failed to connect to the server
            // Invalidate metadata if any
            this.meta_data = null;
            connection = this.getAnyConnection();
        }
        
        return connection;
    }
    
    private boolean hasConnection(ServerAddress server_address) {
        return this.connections.containsKey(server_address);
    }
    
    private KVMessageRaw processRequest(KVMessageRaw kvmsg) throws IOException {
        ServerConnection    connection = this.findConnectionForKey(kvmsg.getKey());
        KVMessageRaw        reply = null;
        
        while (reply == null && connection != null) {
            try {
                logger.info("Sending '" + kvmsg.getStatus().name() + "' request with {key='" + kvmsg.getKey() + "'; value='" +
                        kvmsg.getValue() + "'} to server '" + connection.server_address + "'.");
                reply = connection.processRequest(kvmsg);

            } catch (IOException ex) { // Connection to server is lost
                // Try another connection
                this.connections.remove(connection.server_address);
                connection = this.findConnectionForKey(kvmsg.getKey());
            }
        }
        if (connection == null || reply == null) {
            this.online = false;
            throw new IOException("Unable to connect to any of known service nodes.");
        }
        
        if (reply.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
            logger.info("Server '" + connection.server_address + "' is not responsible for key '" + kvmsg.getKey() +
                    "' (having hash='" + HashValue.hashKey(kvmsg.getKey()) + "'). Updating metadata.");
            this.meta_data = reply.getMetaData();
            logger.debug("Metadata: \n" + this.meta_data);
            reply = this.processRequest(kvmsg);
        } else {
            logger.info("Received reply '" + reply.getStatus().name() + "' with {key='" + reply.getKey() + "'; value='" +
                    reply.getValue() + "'} from server '" + connection.server_address + "'.");
        }
        
        return reply;
    }
    
    private static class ServerConnection {
        private static final Logger logger = LogSetup.getLogger();
        
        private final KVStore       master;
        private final ServerAddress server_address;
        private final Socket        server_socket;
        private final InputStream   input_stream;
        private final OutputStream  output_stream;
        
        public ServerConnection(KVStore master, ServerAddress server_address) throws IOException {
            this.master = master;
            this.server_address = server_address;
            this.server_socket = new Socket(this.server_address.getAddress(), this.server_address.getPort());
            this.input_stream = this.server_socket.getInputStream();
            this.output_stream = this.server_socket.getOutputStream();
        }
        
        public void disconnect() {
            try {
                this.input_stream.close();
                this.output_stream.close();
                if (!this.server_socket.isClosed()) {
                    this.server_socket.close();
                }
            }  catch (IOException ex) {
                logger.warn("Warning! Unable to tear down connection to server (" + this.server_address.toString() +
                        "): " + ex.getMessage());
            }
        }
        
        public KVMessageRaw processRequest(KVMessageRaw kvmsg) throws IOException {
            NetworkMessage  netmsg = new NetworkMessage(KVMessageRaw.marshal(kvmsg));
            
            netmsg.writeTo(this.output_stream);
            
            netmsg = NetworkMessage.readFrom(this.input_stream);
            
            try {
                kvmsg = KVMessageRaw.unmarshal(netmsg.getData());
                
            } catch (ParseException ex) {
                throw new ProtocolException("Failed to parse message from server (" + this.server_address.toString() +
                        "): " + ex.getMessage());
            }
            
            return kvmsg;
        }
    }
}
