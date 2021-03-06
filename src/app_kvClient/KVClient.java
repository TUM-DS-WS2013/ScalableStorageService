package app_kvClient;

import client.KVCommInterface;
import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import common.messages.KVMessage;
import common.parsers.ArgumentParser;
import common.parsers.CommandParser;
import common.parsers.CommandParser.Command;
import common.topology.ServerAddress;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;

public class KVClient {

    private KVCommInterface objKVStoreClient;
    private final Logger logger;

    public KVClient() throws IOException {
        objKVStoreClient = null;
        logger = LogSetup.getLogger();
    }

    /**
     * Main program method
     *
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Level   log_level = Level.WARN;
        String  ip_address = null;
        Integer port = null;
        
        // Parse command line arguments
        try {
            ArgumentParser parser = new ArgumentParser("hl:", args);
            ArgumentParser.Option option;
            
            while ((option = parser.getNextArgument()) != null) {
                if (option.name == null) { // Positional arguments go here
                    if (ip_address == null) {
                        ip_address = option.argument;
                        if (!ServerAddress.validateIPv4Address(ip_address)) {
                            throw new ParseException("Invalid IP address: " + option.argument + ".", 0);
                        }
                    } else if (port == null) {
                        try {
                            port = Integer.parseInt(option.argument);
                        } catch (NumberFormatException e) {}
                        if (port == null || !ServerAddress.validatePortNumber(port)) {
                            throw new ParseException("Invalid port number: " + option.argument + ".", 0);
                        }
                    } else {
                        throw new ParseException("Excess positional argument: " + option.argument + ".", 0);
                    }
                    
                } else if (option.name.equals("h")) {
                    System.out.println(
                            "Usage: KVClient [-l log_level] [ip_address port]\n" +
                            "    -l log_level    - Set logging level (default: WARN).\n" +
                            "    port            - IP address for initial connection.\n" +
                            "    port            - Port number for initial connection."
                      );
                    System.exit(1);
                    
                } else if (option.name.equals("l")) {
                    if (LogSetup.isValidLevel(option.argument)) {
                        log_level = Level.toLevel(option.argument);
                    } else {
                        throw new ParseException("Invalid logging level: " + option.argument + ".", 0);
                    }
                }
            }
            
        } catch (ParseException e) {
            System.out.println("Error parsing command line arguments: " + e.getMessage());
            System.exit(1);
        }
        
        // Initializing logger
        try {
            LogSetup.initialize("logs/client/client.log", log_level);
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger: " + e.getMessage());
            System.exit(1);
        }
        
        try {
            KVClient client = new KVClient();
            CommandParser parser = initializeCommandParser();
            
            if (ip_address != null && port != null) {
                System.out.println(client.ProcessCommand(parser.parseString("connect " + ip_address + " " + port)));
            }
            
            while (true) {
                Command command = parser.nextCommand();
                
                if (command.name.equals("quit")) {
                    break;
                } else {
                    System.out.println(client.ProcessCommand(command));
                }
            }
            
        } catch (IOException exception) {
            System.out.println(exception.getMessage());
        }
    }
    
    /**
     * Creates and initializes a CommandParser instance which will parse user's input from command line.
     * @return A valid CommandParser object
     */
    private static CommandParser initializeCommandParser() {
        CommandParser parser = new CommandParser(System.in, System.out, "KVClient>");
        
        try {
            parser.addCommand("connect <address> <port>",
                    "Connect to storage service running at <address> on port <port>.");
            parser.addCommand("disconnect", "Disconnect from the storage service.");
            parser.addCommand("put <key:20> [value...:122880]",
                    "Store a given <value> for a given <key> in the storage service.");
            parser.addCommand("get <key:20>", "Request a stored value for a given <key> from the storage service.");
            parser.addCommand("logLevel <level>", "Change logging level to <level>.");
            parser.addCommand("quit", "Exit the application.");
            
        } catch (ParseException ex) {
            System.out.println("CommandParser initialization failed: " + ex.getMessage());
            System.exit(1);
        }
        
        return parser;
    }
    
    /**
     * Processes a given command and routes it to a respective module.
     * 
     * @param command Command with arguments returned by the CommandParser
     * @return String describing the process results (suitable for output to user)
     */
    public String ProcessCommand(Command command) {
        String output = null;
        if (objKVStoreClient == null && !command.name.equals("connect")) {
            return "Error: Connection not established!";
        }
        try {
            if (command.name.equals("connect")) {
                objKVStoreClient = new KVStore(command.arguments.get("address"),
                        Integer.parseInt(command.arguments.get("port")));
                objKVStoreClient.connect();
                output = "Connection Established!";
            } else if (command.name.equals("disconnect")) {
                objKVStoreClient.disconnect();
                objKVStoreClient = null;
                output = "Connection terminated!";
            } else if (command.name.equals("logLevel")) {
                output = LogSetup.setLogLevel(command.arguments.get("level"));
            } else if (command.name.equals("quit")) {
                objKVStoreClient.disconnect();
                objKVStoreClient = null;
                output = "Application exit!";
            } else if (command.name.equals("put")) {
                KVMessage message = objKVStoreClient.put(command.arguments.get("key"), command.arguments.get("value"));
                if (message.getStatus() == KVMessage.StatusType.DELETE_SUCCESS)
                    output = "Deletion succeeded: Value was '" + message.getValue() + "'.";
                else if (message.getStatus() == KVMessage.StatusType.PUT_SUCCESS)
                    output = "Value stored successfully. Value is '" + message.getValue() + "'.";
                else if (message.getStatus() == KVMessage.StatusType.PUT_UPDATE)
                    output = "Value updated successfully. Updated value is '" + message.getValue() + "'.";
                else
                    output = "Error occured: " + message.getValue();
            } else if (command.name.equals("get")) {
                KVMessage message = objKVStoreClient.get(command.arguments.get("key"));
                if (message.getStatus() == KVMessage.StatusType.GET_SUCCESS)
                    output = "Stored value is '" + message.getValue() + "'.";
                else
                    output = "Error occured: " + message.getValue();
            }

            logger.info(output);
        } catch (UnknownHostException hostException) {
            output = "Error! Unable to connect to server: " + hostException.getMessage();
            logger.error(output);
        } catch (IOException ioexception) {
            output = "Error! Connection lost: " + ioexception.getMessage();
            if (objKVStoreClient != null) {
                objKVStoreClient.disconnect();
                objKVStoreClient = null;
            }
            logger.error(output);
        } catch (IllegalArgumentException iaexception) {
            output = "Error! " + iaexception.getMessage();
            logger.error(output);
        }catch (Exception exception) {
            output = "Error! Unknown exception: " + exception.getMessage();
            logger.error(output);
        }

        return output;
    }
}
