package app_kvServer;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageRaw;
import common.messages.NetworkMessage;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
    
    private final Socket        client_socket;
    private final KVServer      master;
    private volatile boolean    online;
    private InputStream         input;
    private OutputStream        output;

    /**
     * Main constructor.
     * @param clientSocket An open socket for interaction with client
     * @param master The server instance that created this connection
     */
    public ClientConnection(Socket clientSocket, KVServer master) {
        this.client_socket = clientSocket;
        this.master = master;
        this.online = true;
        this.input = null;
        this.output = null;
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
                    KVMessage       kvmsg, kvmsg_reply;
                    
                    // Process query
                    try {
                        kvmsg = KVMessageRaw.unmarshal(netmsg.getData());
                        
                        logger.info("Received a '" + kvmsg.getStatus().name() + "' request from '" +
                                    client_socket.getInetAddress() + "' with {key='" + kvmsg.getKey() +
                                    "'; value='" + kvmsg.getValue() + "'}.");
                        
                        kvmsg_reply = this.parseKVMessage(kvmsg);
                        
                    } catch (ParseException e) {
                        String report = "Warning! Received KVMessage is invalid: " + e.getMessage();
                        
                        logger.warn(report);
                        kvmsg_reply = new KVMessageRaw(StatusType.PROTOCOL_ERROR, StatusType.PROTOCOL_ERROR.name(), report);
                    }
                    
                    // Send reply
                    logger.info("Replying with '" + kvmsg_reply.getStatus().name() + "': {key='" +
                                kvmsg_reply.getKey() + "'; value='" + kvmsg_reply.getValue() + "'}.");
                    
                    netmsg = new NetworkMessage(KVMessageRaw.marshal(kvmsg_reply));
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
    private KVMessage parseKVMessage(KVMessage kvmsg) throws ParseException {
        StatusType  type = kvmsg.getStatus();
        String      key = kvmsg.getKey();
        String      value = kvmsg.getValue();
        
        StatusType  return_type = null;
        String      return_value = null;
        
        switch (type) {
            case PUT:
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
                break;
                
            case GET:
                return_value = this.master.getDataStorage().get(key);
                if (return_value == null) {
                    return_type = StatusType.GET_ERROR;
                    return_value = "Requested key is not found or invalid.";
                } else {
                    return_type = StatusType.GET_SUCCESS;
                }
                break;
                
            default:
                throw new ParseException("Message type '" + type + "' is not a valid request.", 0);
        }
        
        return new KVMessageRaw(return_type, key, return_value);
    }
}
