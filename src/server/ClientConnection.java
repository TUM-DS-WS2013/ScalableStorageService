package server;

import app_kvServer.KVServer;
import common.messages.ControlMessage;
import common.messages.ControlMessage.ControlType;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageRaw;
import common.messages.NetworkMessage;
import common.topology.ServerAddress;
import common.topology.ServiceMetaData;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ProtocolException;
import java.net.Socket;
import java.text.ParseException;
import logger.LogSetup;
import org.apache.log4j.Logger;

/**
 * A runnable class responsible for interaction with a single client
 * @author Danila KLimenko
 */
public class ClientConnection implements Runnable {
    private static final Logger logger = LogSetup.getLogger();
    private enum Mode {UNINITIALIZED, KVCLIENT_CONNECTION, CONTROL_CONNECTION};
    
    private final Socket        client_socket;
    private final KVServer      master;
    private volatile boolean    online;
    private boolean             shut_down_master;
    private InputStream         input;
    private OutputStream        output;
    private Mode                mode;

    /**
     * Main constructor.
     * @param clientSocket An open socket for interaction with client
     * @param master The server instance that created this connection
     */
    public ClientConnection(Socket clientSocket, KVServer master) {
        this.client_socket = clientSocket;
        this.master = master;
        this.online = true;
        this.shut_down_master = false;
        this.input = null;
        this.output = null;
        this.mode = Mode.UNINITIALIZED;
    }
    
    /**
     * Override for run() method from Runnable interface
     */
    @Override
    public void run() {
        try {
            output = client_socket.getOutputStream();
            input = client_socket.getInputStream();
            
            while (this.online) {
                try {
                    // Receive client's query
                    NetworkMessage  netmsg = NetworkMessage.readFrom(input);
                    
                    if (this.mode == Mode.UNINITIALIZED) {
                        this.mode = (ControlMessage.isControlMessage(netmsg.getData())) ? 
                                Mode.CONTROL_CONNECTION : Mode.KVCLIENT_CONNECTION;
                    }
                    
                    // Process query
                    if (this.mode == Mode.KVCLIENT_CONNECTION) {
                        netmsg = this.processKVMessage(netmsg);
                    } else {
                        netmsg = this.processControlMessage(netmsg);
                    }
                    
                    // Send reply
                    netmsg.writeTo(output);
                    
                } catch (IOException e) {
                    logger.error("Error! Connection lost: " + e.getMessage());
                    this.online = false;
                }
            }
            
        } catch (IOException e) {
            logger.error("Error! Connection could not be established: " + e.getMessage());

        } finally {

            this.closeConnection();
            
            this.master.clientTerminated(this);
        }
        
        if (this.shut_down_master) {
            this.master.shutDown();
        }
    }
    
    /**
     * Closes connection and frees all associated resources.
     */
    public void closeConnection() {
        this.online = false;
        
        try {
            if (this.input != null) {
                this.input.close();
                this.input = null;
            }
            if (this.output != null) {
                this.output.close();
                this.output = null;
            }
            if (!this.client_socket.isClosed()) {
                this.client_socket.close();
            }
        } catch (IOException e) {
            logger.error("Error! Unable to tear down connection: " + e.getMessage());
        }
    }
    
    /**
     * Parses the query received from client, updates or requests data from the
     * key-value data storage, and generates a reply-message.
     * @param kvmsg Client's query in a form of KVMessage
     * @return KVMessage representing the reply for the client
     * @throws ParseException Thrown if client's query contains illegal data
     */
    private NetworkMessage processKVMessage(NetworkMessage netmsg) throws IOException {
        KVMessage       kvmsg;
        StatusType      return_type;
        String          return_value;
        ServiceMetaData meta_data = null;
        
        try {
            kvmsg = KVMessageRaw.unmarshal(netmsg.getData());

        } catch (ParseException e) {
            return_type = StatusType.PROTOCOL_ERROR;
            return_value = "Warning! Received KVMessage is invalid: " + e.getMessage();

            logger.warn(return_value);
            
            return new NetworkMessage(KVMessageRaw.marshal(new KVMessageRaw(return_type, return_type.name(), return_value)));
        }
        
        StatusType  type = kvmsg.getStatus();
        String      key = kvmsg.getKey();
        String      value = kvmsg.getValue();
        
        logger.info("Server '" + this.master.getAddressAsString() + "': Received a '" + kvmsg.getStatus().name() +
                "' request from '" + this.client_socket.getInetAddress() + "' with {key='" + kvmsg.getKey() +
                "'; value='" + kvmsg.getValue() + "'}.");
        
        if (type != StatusType.PUT && type != StatusType.GET) {
            return_type = StatusType.PROTOCOL_ERROR;
            return_value = "Message type '" + type + "' is not a valid request.";
            
        } else if (type == StatusType.GET && this.master.isStopped()) {
            return_type = StatusType.SERVER_STOPPED;
            return_value = "Server is currently stopped. All read and write operations are rejected.";
            
        } else if (type == StatusType.PUT && this.master.isLocked()) {
            return_type = StatusType.SERVER_WRITE_LOCK;
            return_value = "Server is currently locked. All write operations are rejected.";
            
        } else if (!this.master.isResponsibleForKey(key)) {
            return_type = StatusType.SERVER_NOT_RESPONSIBLE;
            return_value = "Server is not responsible for the provided key. Forwarding metadata update.";
            meta_data = this.master.getMetaData();
            
        } else if (type == StatusType.PUT) {
            if (value != null) { // Performing put operation
                try {
                    return_value = this.master.getDataStorage().put(key, value);
                    return_type = (return_value == null) ?
                                    StatusType.PUT_SUCCESS : StatusType.PUT_UPDATE;
                    return_value = value; // Return the value form the client query

                } catch (IllegalArgumentException e) {
                    return_type = StatusType.PUT_ERROR;
                    return_value = e.getMessage();
                }

            } else { // Performing delete operation
                return_value = this.master.getDataStorage().delete(key);
                if (return_value == null) {
                    return_type = StatusType.DELETE_ERROR;
                    return_value = "Requested key is not found or invalid.";
                } else {
                    return_type = StatusType.DELETE_SUCCESS;
                }
            }
            
        } else {
            return_value = this.master.getDataStorage().get(key);
            if (return_value == null) {
                return_type = StatusType.GET_ERROR;
                return_value = "Requested key is not found or invalid.";
            } else {
                return_type = StatusType.GET_SUCCESS;
            }
        }
        
        logger.info("Server '" + this.master.getAddressAsString() + "': Replying with '" + return_type.name() +
                "': {key='" + kvmsg.getKey() + "'; value='" + return_value + "'; metadata='" +
                (meta_data == null ? "NO" : "YES") + "'}.");
        
        return new NetworkMessage(KVMessageRaw.marshal(new KVMessageRaw(return_type, key, return_value, meta_data)));
    }
    
    private NetworkMessage processControlMessage(NetworkMessage netmsg) throws IOException {
        ControlMessage  ctrlmsg;
        
        try {
            ctrlmsg = ControlMessage.unmarshal(netmsg.getData());

        } catch (ParseException e) {
            String description = "Warning! Received ControlMessage is invalid: " + e.getMessage();
            logger.warn(description);
            return new NetworkMessage(ControlMessage.marshal(new ControlMessage(ControlType.FAILURE, description)));
        }
        
        ControlType type = ctrlmsg.getType();
        
        logger.info("Server '" + this.master.getAddressAsString() + "': Received a control message of type '" + type.name() +
                "' from '" + this.client_socket.getInetAddress() + ":" + this.client_socket.getPort() + "'.");
        
        try {
            switch (type) {
                case INIT:
                    ServerAddress   server_address = null;
                    try {
                        server_address = new ServerAddress(ctrlmsg.getDescription());
                    } catch (ParseException ex) {
                        throw new ProtocolException("Bad server address in INIT message: " + ex.getMessage());
                    }
                    this.master.initialize(server_address, ctrlmsg.getMetaData());
                    break;

                case START:
                    this.master.start();
                    break;

                case STOP:
                    this.master.stop();
                    break;

                case SHUTDOWN:
                    this.online = false;
                    this.shut_down_master = true;
                    break;

                case LOCK_WRITE:
                    this.master.lockWrite();
                    break;

                case UNLOCK_WRITE:
                    this.master.unlockWrite();
                    break;

                case UPDATE:
                    this.master.updateMetaData(ctrlmsg.getMetaData());
                    break;

                case MOVE_DATA:
                    this.master.moveData(ctrlmsg.getDataTransferRequest());
                    break;

                case DELETE_DATA:
                    this.master.deleteData(ctrlmsg.getDataTransferRequest());
                    break;

                case TRANSFER:
                    this.master.acceptTransferredData(ctrlmsg.getKeyValuePacket());
                    break;
                    
                default:
                    throw new ProtocolException("Received a control message with invalid type: '" + type.name() + "'.");
            }
            
            ctrlmsg = new ControlMessage(ControlType.SUCCESS);
            
            logger.info("Server '" + this.master.getAddressAsString() + "': Replying to a control message ('" + type.name() +
                    "') with '" + ctrlmsg.getType().name() + "'.");
            
        } catch (ProtocolException ex) {
            ctrlmsg = new ControlMessage(ControlType.FAILURE, ex.getMessage());
            logger.error("Error processing control message: " + ex.getMessage());
            
        } catch (IllegalStateException ex) {
            ctrlmsg = new ControlMessage(ControlType.FAILURE, ex.getMessage());
            logger.error("Error processing control message: " + ex.getMessage());
        }
        
        return new NetworkMessage(ControlMessage.marshal(ctrlmsg));
    }
}
