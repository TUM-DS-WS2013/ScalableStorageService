package common.topology;

import java.text.ParseException;

/**
 *
 * @author Danila Klimenko
 */
public class ServerAddress {
    private final String    address;
    private final int       port;
    
    public ServerAddress(String address, int port) {
        if (!ServerAddress.validateIPv4Address(address) || !ServerAddress.validatePortNumber(port)) {
            throw new IllegalArgumentException("ServerAddress(): IP address (" + address + ") or port (" + port + ") are invalid.");
        }
        
        this.address = address;
        this.port = port;
    }
    
    public ServerAddress(String address_and_port) throws ParseException {
        String[] tokens = address_and_port.split(":");
        if (tokens.length != 2) {
            throw new ParseException("Bad server address string format.", 0);
        }
        
        this.address = tokens[0];
        this.port = Integer.parseInt(tokens[1]);
        
        if (this.port < 0 || this.port > 65535) {
            throw new ParseException("Illegal port number: " + this.port + ".", 0);
        }
    }
    
    public String getAddress() {
        return this.address;
    }
    
    public int getPort() {
        return this.port;
    }
    
    public static boolean   validateIPv4Address(String ip_string) {
        if (ip_string == null) {
            return false;
        }
        
        String[]    ip_parts = ip_string.split("\\.");
        
        if (ip_parts.length != 4) {
            return false;
        }
        
        try {
            for (String str : ip_parts) {
                int ip_part = Integer.parseInt(str);
                if (ip_part < 0 || ip_part > 255) {
                    return false;
                }
            }
        } catch (NumberFormatException ex) {
            return false;
        }
        
        return true;
    }
    
    public static boolean   validatePortNumber(int port) {
        return !(port < 0 || port > 65535);
    }

    @Override
    public String toString() {
        return this.address + ":" + Integer.toString(this.port);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof ServerAddress))
            return false;

        ServerAddress   other = (ServerAddress) obj;
        return this.address.equals(other.address) && (this.port == other.port);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 61 * hash + (this.address != null ? this.address.hashCode() : 0);
        hash = 61 * hash + this.port;
        return hash;
    }
}
