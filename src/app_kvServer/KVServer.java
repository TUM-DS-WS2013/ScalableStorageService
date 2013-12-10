package app_kvServer;

import common.parsers.ArgumentParser;
import common.parsers.CommandParser;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Set;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class KVServer implements Runnable {
    private static final Logger logger = LogSetup.getLogger();
    private final int           port;
    
    private final KVDataStorage         data_storage;
    private final ServerSocket          server_socket;
    private final Set<ClientConnection> clients;
    private volatile boolean            online;
    
    /**
     * Constructor taking port number as its only argument
     * @param port Port number
     * @throws IOException Thrown if server socket cannot be created
     */
    public KVServer(int port) throws IOException {
        this.port = port;
        this.online = false;
        
        logger.info("Initializing server ...");
        this.server_socket = new ServerSocket(this.port);
        
        this.data_storage = new KVDataStorage();
        this.clients = new HashSet<ClientConnection>();
        
        logger.info("Server listening on port: " + this.server_socket.getLocalPort());
        this.online = true;
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
                if (option.name == null) { // Positional argument go here
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
            
            if (port == null) {
                throw new ParseException("Port number is not provided.", 0);
            }
            
        } catch (ParseException e) {
            System.out.println("Error parsing command line arguments: " + e.getMessage());
            printUsage();
            System.exit(1);
        }
        
        // Initialize logger
        try {
            LogSetup.initialize("logs/server/server.log", log_level);
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger: " + e.getMessage());
            System.exit(1);
        }
        
        // Start server
        try {
            KVServer server = new KVServer(port);
            new Thread(server).start();
            
            CommandParser parser = initializeCommandParser();
            
            while (server.online) {
                CommandParser.Command command = parser.nextCommand();
                
                if (command.name.equals("dump")) {
                    System.out.println(server.data_storage.dump());
                    
                } if (command.name.equals("log")) {
                    System.out.println(LogSetup.setLogLevel(command.arguments.get("level")));
                
                } if (command.name.equals("quit")) {
                    server.shutDown();
                }
            }
            
        } catch (IOException e) {
            logger.error("Error! Cannot start server: " + e.getMessage());
        }
    }
    
    /**
     * Print help text
     */
    private static void printUsage() {
        System.out.println(
                  "Usage: KVServer [-l log_level] <port>\n"
                + "    -l log_level    - Set logging level (default: WARN).\n"
                + "    <port>          - Port number for listening for connections."
        );
    }
    
    private static CommandParser initializeCommandParser() {
        CommandParser parser = new CommandParser(System.in, System.out, ">");
        
        try {
            parser.addCommand("dump", "Print the data stored on the server.");
            parser.addCommand("log <level>", "Change the logging level to <level>.");
            parser.addCommand("quit", "Exit the application.");
            
        } catch (ParseException ex) {
            System.out.println("CommandParser initialization failed: " + ex.getMessage());
            System.exit(1);
        }
        
        return parser;
    }
}
