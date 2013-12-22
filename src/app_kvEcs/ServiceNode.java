package app_kvEcs;

import common.messages.ControlMessage;
import common.messages.NetworkMessage;
import common.topology.HashValue;
import common.topology.ServerAddress;
import common.topology.ServiceMetaData;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import server.DataTransferRequest;

/**
 *
 * @author Danila Klimenko
 */
public class ServiceNode {
    private static final Logger logger = LogSetup.getLogger();
    
    private final String        name;
    private final ServerAddress server_address;
    private final String        path_to_jar;
    
    private boolean         connected;
    private Socket          socket;
    private InputStream     input_stream;
    private OutputStream    output_stream;

    public ServiceNode(String name, ServerAddress server_address, String path_to_jar) {
        this.name = name;
        this.server_address = server_address;
        this.path_to_jar = path_to_jar;
        
        this.connected = false;
        this.socket = null;
        this.input_stream = null;
        this.output_stream = null;
    }
    
    public ServiceNode(String name, String address, int port, String path_to_jar) {
        this.name = name;
        this.server_address = new ServerAddress(address, port);
        this.path_to_jar = path_to_jar;
        
        this.connected = false;
        this.socket = null;
        this.input_stream = null;
        this.output_stream = null;
    }

    public String getName() {
        return this.name;
    }
    
    public ServerAddress getServerAddress() {
        return this.server_address;
    }
    
    public void initialize(ServiceMetaData meta_data, Level log_level) throws IOException {
        // Launch server via ssh
        List<String>    launch_command = new ArrayList<String>();
        
        launch_command.add("ssh");
        launch_command.add("-n");
        launch_command.add(this.server_address.getAddress());
        launch_command.add("nohup");
        launch_command.add("java");
        launch_command.add("-jar");
        launch_command.add(this.path_to_jar);
        launch_command.add("-l");
        launch_command.add(log_level.toString());
        launch_command.add(Integer.toString(this.server_address.getPort()));
        launch_command.add("&");
        
        logger.info("Launching a remote server at '" + this.server_address + "'.");
        new ProcessBuilder(launch_command).start();
        
        try {
            // Connect to server
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {}
            this.socket = new Socket(this.server_address.getAddress(), this.server_address.getPort());
            this.input_stream = this.socket.getInputStream();
            this.output_stream = this.socket.getOutputStream();
            this.connected = true;
            
            // Send metadata to server
            ControlMessage  ctrlmsg = new ControlMessage(ControlMessage.ControlType.INIT, this.server_address.toString(), meta_data);
            
            ctrlmsg = this.processControlMessage(ctrlmsg);
            
            if (ctrlmsg.getType() == ControlMessage.ControlType.FAILURE) {
                throw new IOException("Remote server '" + this.server_address + "' failed to initialize: " +
                        ctrlmsg.getDescription() + ".");
            }
            
        } catch (IOException ex) {
            this.shutDown();
            throw ex;
        }
        logger.info("Remote server at '" + this.server_address + "' launched successfully.");
    }
    
    public void updateMetaData(ServiceMetaData meta_data) throws IOException {
        ControlMessage  ctrlmsg = new ControlMessage(ControlMessage.ControlType.UPDATE, meta_data);
        ctrlmsg = this.processControlMessage(ctrlmsg);
        if (ctrlmsg.getType() == ControlMessage.ControlType.FAILURE) {
            throw new IOException("Remote server '" + this.server_address + "' failed to update metadata: " +
                    ctrlmsg.getDescription() + ".");
        }
        logger.info("Updated metadata on remote server '" + this.server_address + "'.");
    }
    
    public void start() throws IOException {
        ControlMessage  ctrlmsg = new ControlMessage(ControlMessage.ControlType.START);
        ctrlmsg = this.processControlMessage(ctrlmsg);
        if (ctrlmsg.getType() == ControlMessage.ControlType.FAILURE) {
            throw new IOException("Remote server '" + this.server_address + "' failed to start: " +
                    ctrlmsg.getDescription() + ".");
        }
        logger.info("Started remote server '" + this.server_address + "'.");
    }
    
    public void stop() throws IOException {
        ControlMessage  ctrlmsg = new ControlMessage(ControlMessage.ControlType.STOP);
        ctrlmsg = this.processControlMessage(ctrlmsg);
        if (ctrlmsg.getType() == ControlMessage.ControlType.FAILURE) {
            throw new IOException("Remote server '" + this.server_address + "' failed to stop: " +
                    ctrlmsg.getDescription() + ".");
        }
        logger.info("Stopped remote server '" + this.server_address + "'.");
    }
    
    public void shutDown() throws IOException {
        try {
            if (this.connected && !this.socket.isOutputShutdown()) {
                ControlMessage  ctrlmsg = new ControlMessage(ControlMessage.ControlType.SHUTDOWN);
                NetworkMessage  netmsg = new NetworkMessage(ControlMessage.marshal(ctrlmsg));
                netmsg.writeTo(this.output_stream);
            }
            if (this.input_stream != null) {
                this.input_stream.close();
            }
            if (this.output_stream != null) {
                this.output_stream.close();
            }
            if (this.socket != null && !this.socket.isClosed()) {
                this.socket.close();
            }
        }  catch (IOException ex) {
            logger.warn("Warning! Unable to tear down connection to server (" + this.server_address + "): " + ex.getMessage());
        }
        this.connected = false;
        logger.info("Remote server at '" + this.server_address + "' shut down.");
    }
    
    public void lockWrite() throws IOException {
        ControlMessage  ctrlmsg = new ControlMessage(ControlMessage.ControlType.LOCK_WRITE);
        ctrlmsg = this.processControlMessage(ctrlmsg);
        if (ctrlmsg.getType() == ControlMessage.ControlType.FAILURE) {
            throw new IOException("Remote server '" + this.server_address + "' failed to lock write operations: " +
                    ctrlmsg.getDescription() + ".");
        }
        logger.info("Locked write operations on remote server '" + this.server_address + "'.");
    }
    
    public void unlockWrite() throws IOException {
        ControlMessage  ctrlmsg = new ControlMessage(ControlMessage.ControlType.UNLOCK_WRITE);
        ctrlmsg = this.processControlMessage(ctrlmsg);
        if (ctrlmsg.getType() == ControlMessage.ControlType.FAILURE) {
            throw new IOException("Remote server '" + this.server_address +
                    "' failed to unlock write operations: " + ctrlmsg.getDescription() + ".");
        }
        logger.info("Unlocked write operations on remote server '" + this.server_address + "'.");
    }
    
    public void moveData(HashValue range_begin, HashValue range_end, ServerAddress target) throws IOException {
        DataTransferRequest dt_request = new DataTransferRequest(range_begin, range_end, target);
        ControlMessage      ctrlmsg = new ControlMessage(ControlMessage.ControlType.MOVE_DATA, dt_request);
        ctrlmsg = this.processControlMessage(ctrlmsg);
        if (ctrlmsg.getType() == ControlMessage.ControlType.FAILURE) {
            throw new IOException("Remote server '" + this.server_address + "' failed to transfer data to '" +
                    target + "': " + ctrlmsg.getDescription() + ".");
        }
        logger.info("Moved data from server '" + this.server_address + "' to server '" + target + "'.");
    }
    
    public void deleteData(HashValue range_begin, HashValue range_end) throws IOException {
        DataTransferRequest dt_request = new DataTransferRequest(range_begin, range_end, this.server_address);
        ControlMessage      ctrlmsg = new ControlMessage(ControlMessage.ControlType.DELETE_DATA, dt_request);
        ctrlmsg = this.processControlMessage(ctrlmsg);
        if (ctrlmsg.getType() == ControlMessage.ControlType.FAILURE) {
            throw new IOException("Remote server '" + this.server_address + "' failed to delete data: " + ctrlmsg.getDescription() + ".");
        }
        logger.info("Deleted data on server '" + this.server_address + "'.");
    }
    
    private ControlMessage processControlMessage(ControlMessage ctrlmsg) throws IOException {
        NetworkMessage  netmsg = new NetworkMessage(ControlMessage.marshal(ctrlmsg));
            
        netmsg.writeTo(this.output_stream);
        netmsg = NetworkMessage.readFrom(this.input_stream);

        try {
            ctrlmsg = ControlMessage.unmarshal(netmsg.getData());
        } catch (ParseException ex) {
            throw new IOException("Received an invalid message from remote server '" +
                    this.server_address + "': " + ex.getMessage() + ".");
        }
        
        if (ctrlmsg.getType() != ControlMessage.ControlType.SUCCESS &&
                ctrlmsg.getType() != ControlMessage.ControlType.FAILURE) {
            throw new IOException("Remote server '" + this.server_address + "' does not follow the protocol.");
        }
        
        return ctrlmsg;
    }
    
    public static List<ServiceNode> parseServiceNodesFromFile(String config_path) throws ParseException, IOException {
        BufferedReader      reader = new BufferedReader(new FileReader(config_path));
        String              string = reader.readLine();
        Pattern             node_descr_pattern = Pattern.compile(
                "([_a-zA-Z0-9]+)\\s+(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})\\s+(\\d{1,5})\\s+(\\S+)");
        List<ServiceNode>   nodes = new ArrayList<ServiceNode>();
        
        while (string != null) {
            string = string.trim();
            
            if ((string.length() > 0) && !string.startsWith("#")) {
                Matcher matcher = node_descr_pattern.matcher(string);
                
                if (!matcher.matches()) {
                    throw new ParseException("Illegal string format in configurational file: '" + string + "'", 0);
                }
                
                String      ip_address = matcher.group(2);
                if (!ServerAddress.validateIPv4Address(ip_address)) {
                    throw new ParseException("Illegal IP address in configurational file: '" + string + "'", 0);
                }
                
                int port = Integer.parseInt(matcher.group(3));
                if (!ServerAddress.validatePortNumber(port)) {
                    throw new ParseException("Illegal port number in configurational file: '" + string + "'", 0);
                }
                
                nodes.add(new ServiceNode(matcher.group(1), ip_address, port, matcher.group(4)));
            }
            
            string = reader.readLine();
        }
        
        return nodes;
    }

    @Override
    public String toString() {
        return "Node '" + this.name + "': Address='" + this.server_address + "'; Connected='" + this.connected + "'";
    }
}
