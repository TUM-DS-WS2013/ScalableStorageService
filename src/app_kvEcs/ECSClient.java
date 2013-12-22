package app_kvEcs;

import app_kvServer.KVServer;
import common.parsers.ArgumentParser;
import common.parsers.CommandParser;
import common.topology.HashValue;
import common.topology.ServerAddress;
import common.topology.ServiceMetaData;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ECSClient implements Runnable {
    private enum ServiceState {UNINITIALIZED, STOPPED, RUNNING};
    private static final Logger logger = LogSetup.getLogger();
    
    private final List<ServiceNode> active_nodes;
    private final List<ServiceNode> inactive_nodes;
    private ServiceState            state;
    private ServiceMetaData         meta_data;

    public ECSClient(List<ServiceNode> nodes) {
        this.active_nodes = new ArrayList<ServiceNode>();
        this.inactive_nodes = nodes;
        this.state = ServiceState.UNINITIALIZED;
        this.meta_data = null;
    }
    
    private void initializeNodes(int num_nodes) throws IllegalStateException, IllegalArgumentException, IOException {
        if (this.state != ServiceState.UNINITIALIZED) {
            throw new IllegalStateException("Service is already initialized.");
        }
        if (num_nodes < 1 || num_nodes > this.inactive_nodes.size()) {
            throw new IllegalArgumentException("Illegal number of nodes (must be in range [1, " +
                    this.inactive_nodes.size() + "]).");
        }
        
        Collections.shuffle(this.inactive_nodes);
        
        int i = 0;
        for (Iterator<ServiceNode> it = this.inactive_nodes.iterator(); it.hasNext() && (i < num_nodes); ++i) {
            ServiceNode node = it.next();
            this.active_nodes.add(node);
            it.remove();
        }
        
        List<ServerAddress> active_addresses = new ArrayList<ServerAddress>(this.active_nodes.size());
        for (ServiceNode node : this.active_nodes) {
            active_addresses.add(node.getServerAddress());
        }
        
        this.meta_data = ServiceMetaData.generateForServers(active_addresses);
        
        for (ServiceNode node : this.active_nodes) {
            node.initialize(this.meta_data, Level.DEBUG);
        }
        
        this.state = ServiceState.STOPPED;
    }
    
    private void startService() throws IllegalStateException, IOException {
        if (this.state != ServiceState.STOPPED) {
            throw new IllegalStateException("Service is not initialized or already running.");
        }
        
        for (ServiceNode node : this.active_nodes) {
            node.start();
        }
        
        this.state = ServiceState.RUNNING;
    }
    
    private void stopService() throws IllegalStateException, IOException {
        if (this.state != ServiceState.RUNNING) {
            throw new IllegalStateException("Service is not initialized or already running.");
        }
        
        for (ServiceNode node : this.active_nodes) {
            node.stop();
        }
        
        this.state = ServiceState.STOPPED;
    }
    
    private void shutDownService() throws IllegalStateException, IOException {
        if (this.state != ServiceState.UNINITIALIZED) {
            for (ServiceNode node : this.active_nodes) {
                node.shutDown();
                this.inactive_nodes.add(node);
            }
            this.active_nodes.clear();
        }
        
        this.state = ServiceState.UNINITIALIZED;
    }
    
    private void addNode() throws IOException {
        if (this.state != ServiceState.RUNNING) {
            throw new IllegalStateException("Cannot add a node to an uninitialized or stopped service.");
        }
        if (this.inactive_nodes.isEmpty()) {
            throw new IllegalStateException("All available nodes are already active.");
        }
        
        ServiceNode     added_node = this.inactive_nodes.remove(0);
        ServerAddress   added_address = added_node.getServerAddress();
        
        this.meta_data.addServer(added_address);
        
        HashValue[]     added_node_hash_range = this.meta_data.getHashRangeForServer(added_address);
        ServerAddress   successor_address = this.meta_data.getSuccessorAddressForServer(added_address);
        ServiceNode     successor_node = this.activeNodeWithAddress(successor_address);
        
        added_node.initialize(this.meta_data, Level.DEBUG);
        this.active_nodes.add(added_node);
        added_node.start();
        
        successor_node.lockWrite();
        successor_node.moveData(added_node_hash_range[0], added_node_hash_range[1], added_address);
        
        for (ServiceNode node : this.active_nodes) {
            node.updateMetaData(this.meta_data);
        }
        
        successor_node.deleteData(added_node_hash_range[0], added_node_hash_range[1]);
        successor_node.unlockWrite();
    }
    
    private void removeNode() throws IOException {
        if (this.state != ServiceState.RUNNING) {
            throw new IllegalStateException("Cannot remove a node from an uninitialized or stopped service.");
        }
        if (this.active_nodes.size() <= 1) {
            throw new IllegalStateException("Cannot remove the last active node.");
        }
        
        ServiceNode     removed_node = this.active_nodes.remove(new Random().nextInt(this.active_nodes.size()));
        ServerAddress   removed_address = removed_node.getServerAddress();
        
        HashValue[]     removed_node_hash_range = this.meta_data.getHashRangeForServer(removed_address);
        ServerAddress   successor_address = this.meta_data.getSuccessorAddressForServer(removed_address);
        ServiceNode     successor_node = this.activeNodeWithAddress(successor_address);
        
        this.meta_data.removeServer(removed_address);
        
        removed_node.lockWrite();
        successor_node.updateMetaData(this.meta_data);
        
        removed_node.moveData(removed_node_hash_range[0], removed_node_hash_range[1], successor_address);
        
        for (ServiceNode node : this.active_nodes) {
            node.updateMetaData(this.meta_data);
        }
        
        removed_node.shutDown();
        this.inactive_nodes.add(removed_node);
    }
    
    private ServiceNode activeNodeWithAddress(ServerAddress server_address) {
        ServiceNode ret_node = null;
        
        for (ServiceNode node : this.active_nodes) {
            if (node.getServerAddress() == server_address) {
                ret_node = node;
                break;
            }
        }
        
        if (ret_node == null) {
            logger.fatal("FATAL ERROR! Trying to access an uninitialized node '" + server_address + "'!");
            System.exit(-1);
        }
        
        return ret_node;
    }
    
    private void dumpService() {
        if ((this.state != ServiceState.RUNNING) || this.active_nodes.isEmpty()) {
            return;
        }
        
        KVServer        dump_server = null;
        try {
            ServerAddress   dump_address = new ServerAddress("127.0.0.1" ,65432);
            dump_server = new KVServer(dump_address.getPort());
            
            Thread          dump_server_thread = new Thread(dump_server);
            dump_server_thread.start();
            
            dump_server.startDumpServer();
            
            for (ServiceNode node : this.active_nodes) {
                HashValue[] node_range = this.meta_data.getHashRangeForServer(node.getServerAddress());
                
                node.lockWrite();
                node.moveData(node_range[0], node_range[1], dump_address);
                node.unlockWrite();
                
                System.out.println("Node '" + node.getServerAddress() + "': " + dump_server.getDataStorage().dump());
                dump_server.getDataStorage().deleteHashRange(node_range[0], node_range[1]);
            }
            
        } catch (IOException ex) {
            System.out.println("Error! Failed to dump the service: " + ex.getMessage());
        } finally {
            if (dump_server != null) {
                dump_server.shutDown();
            }
        }
    }
    
    @Override
    public void run() {
        try {
            CommandParser           parser = initializeCommandParser();
            CommandParser.Command   command;
            
            while (!(command = parser.nextCommand()).name.equals("quit")) {
                try {
                    if (command.name.equals("init")) {
                        int num_nodes;
                        try {
                            num_nodes = Integer.parseInt(command.arguments.get("number_of_nodes"));
                        } catch (NumberFormatException ex) {
                            System.out.println("Error! 'initService' command requires an integer argument.");
                            continue;
                        }

                        this.initializeNodes(num_nodes);

                    } else if (command.name.equals("start")) {
                        this.startService();

                    } else if (command.name.equals("stop")) {
                        this.stopService();

                    } else if (command.name.equals("shutDown")) {
                        this.shutDownService();

                    } else if (command.name.equals("addNode")) {
                        this.addNode();

                    } else if (command.name.equals("removeNode")) {
                        this.removeNode();

                    } else if (command.name.equals("state")) {
                        System.out.println("State: " + this.state.name());
                        System.out.println("Active nodes: ");
                        for (ServiceNode node : this.active_nodes) {
                            System.out.println("    " + node);
                        }
                        System.out.println("Inactive nodes: ");
                        for (ServiceNode node : this.inactive_nodes) {
                            System.out.println("    " + node);
                        }
                        System.out.println("Metadata: \n" + this.meta_data);

                    } else if (command.name.equals("log")) {
                        System.out.println(LogSetup.setLogLevel(command.arguments.get("level")));
                    } else if (command.name.equals("dump")) {
                        this.dumpService();
                    }
                    
                } catch (IllegalStateException ex) {
                    System.out.println("Error! " + ex.getMessage());
                } catch (IllegalArgumentException ex) {
                    System.out.println("Error! " + ex.getMessage());
                } catch (IOException ex) {
                    logger.error("Critical error: " + ex.getMessage());
                    logger.error("Terminating service.");
                    break;
                }
            }
            this.shutDownService();
            
        } catch (IOException ex) {
            logger.error("Error! Reading user input failed: " + ex.getMessage());
        }
    }
    
    public static void main(String[] args) {
        Level   log_level = Level.WARN;
        String  config_path = null;
        
        // Parse command line arguments
        try {
            ArgumentParser parser = new ArgumentParser("hl:", args);
            ArgumentParser.Option option;
            
            while ((option = parser.getNextArgument()) != null) {
                if (option.name == null) { // Positional arguments go here
                    if (config_path == null) {
                        config_path = option.argument;
                    } else {
                        throw new ParseException("Excess positional argument: " + option.argument + ".", 0);
                    }
                    
                } else if (option.name.equals("h")) {
                    printUsage();
                    System.exit(1);
                    
                } else if (option.name.equals("l")) {
                    if (LogSetup.isValidLevel(option.argument)) {
                        log_level = Level.toLevel(option.argument);
                    } else {
                        throw new ParseException("Invalid logging level: " + option.argument + ".", 0);
                    }
                }
            }
            
            if (config_path == null) {
                throw new ParseException("Path to configarational file is not provided.", 0);
            }
            
        } catch (ParseException e) {
            System.out.println("Error parsing command line arguments: " + e.getMessage());
            printUsage();
            System.exit(1);
        }
        
        // Initialize logger
        try {
            LogSetup.initialize("logs/ecs/ecs.log", log_level);
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger: " + e.getMessage());
            System.exit(1);
        }
        
        // Read configurational file
        List<ServiceNode>   nodes = null;
        try {
            nodes = ServiceNode.parseServiceNodesFromFile(config_path);
        } catch (ParseException ex) {
            logger.error("Error parsing configurational file: " + ex.getMessage());
            System.exit(1);
        } catch (FileNotFoundException ex) {
            logger.error("Error! Unable to open the configurational file: " + ex.getMessage());
            System.exit(1);
        } catch (IOException ex) {
            logger.error("Error reading configurational file: " + ex.getMessage());
            System.exit(1);
        }
        
        // Initialize and run ECSClient
        ECSClient   ecsclient = new ECSClient(nodes);
        ecsclient.run();
    }
    
    private static void printUsage() {
        System.out.println(
                  "Usage: ECSClient [-l log_level] <config_file>\n"
                + "    -l log_level    - Set logging level (default: WARN).\n"
                + "    <config_file>   - Path to configurational file."
        );
    }
    
    private static CommandParser initializeCommandParser() {
        CommandParser parser = new CommandParser(System.in, System.out, "ECSClient>");
        
        try {
            parser.addCommand("init <number_of_nodes>",
                    "Start up and initialize the storage service consisting of <number_of_nodes> servers.");
            parser.addCommand("start", "Start the storage service.");
            parser.addCommand("stop", "Stop the storage service.");
            parser.addCommand("shutDown", "Stop the storage service and shut down all the servers.");
            parser.addCommand("addNode", "Add a new node to the storage service at an arbitrary position.");
            parser.addCommand("removeNode", "Remove a node from the storage service at an arbitrary position.");
            parser.addCommand("state", "Print out state of service and its nodes.");
            parser.addCommand("log <level>", "Change the logging level to <level>.");
            parser.addCommand("dump", "Print the contents of every active node in the system.");
            parser.addCommand("quit", "Exit the application.");
            
        } catch (ParseException ex) {
            System.out.println("CommandParser initialization failed: " + ex.getMessage());
            System.exit(1);
        }
        
        return parser;
    }
}
