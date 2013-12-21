package common.topology;

import common.messages.Marshaller;
import common.messages.Unmarshaller;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import logger.LogSetup;
import org.apache.log4j.Logger;

/**
 *
 * @author Danila Klimenko
 */
public class ServiceMetaData {
    private static final Logger logger = LogSetup.getLogger();
    
    private final Map<ServerAddress, MetaDataItem>  records;
    
    public static ServiceMetaData generateForServers(List<ServerAddress> server_addresses) {
        Map<ServerAddress, MetaDataItem>    records = new HashMap<ServerAddress, MetaDataItem>(server_addresses.size());
        List<AddressHashPair>               ah_pairs = new ArrayList<AddressHashPair>(server_addresses.size());
        
        for (ServerAddress server_address : server_addresses) {
            ah_pairs.add(new AddressHashPair(server_address, HashValue.hashServerAddress(server_address)));
        }
        Collections.sort(ah_pairs);
        
        HashValue               prev_hash = ah_pairs.get(ah_pairs.size() - 1).hash;
        for (AddressHashPair pair : ah_pairs) {
            records.put(pair.server_address, new MetaDataItem(pair.server_address, prev_hash, pair.hash));
            prev_hash = pair.hash;
        }
        
        return new ServiceMetaData(records);
    }
    
    public ServiceMetaData(ServiceMetaData other) {
        this.records = new HashMap<ServerAddress, MetaDataItem>(other.records);
    }
    
    private ServiceMetaData(Map<ServerAddress, MetaDataItem> records) {
        this.records = records;
    }
    
    public ServiceMetaData addServer(ServerAddress server_address) {
        MetaDataItem    successor = findSuccessorForServer(server_address, false);
        
        HashValue       new_server_begin_hash = successor.begin_hash;
        HashValue       new_server_end_hash = HashValue.hashServerAddress(server_address);
        
        successor.begin_hash = new_server_end_hash;
        
        this.records.put(server_address, new MetaDataItem(server_address, new_server_begin_hash, new_server_end_hash));
        
        return this;
    }
    
    public ServiceMetaData removeServer(ServerAddress server_address) {
        MetaDataItem    server = findServer(server_address);
        MetaDataItem    successor = findSuccessorForServer(server_address, true);
        
        successor.begin_hash = server.begin_hash;
        
        if (this.records.remove(server_address) == null) {
            logger.fatal("FATAL ERROR! Failed removing server '" + server_address + "' from metadata!");
            System.exit(-1);
        }
        
        return this;
    }
    
    private MetaDataItem findServer(ServerAddress server_address) {
        MetaDataItem    server = this.records.get(server_address);
        
        if (server == null) {
            logger.fatal("FATAL ERROR! Address '" + server_address + "' is not in metadata!");
            System.exit(-1);
        }
        
        return server;
    }
    
    private MetaDataItem findSuccessorForServer(ServerAddress server_address, boolean active) {
        HashValue       server_hash = HashValue.hashServerAddress(server_address);
        MetaDataItem    successor = null;
        
        for (MetaDataItem item: this.records.values()) {
            if ((active && (item.begin_hash.compareTo(server_hash) == 0)) ||
                    (!active && server_hash.isInRange(item.begin_hash, item.end_hash))) {
                successor = item;
                break;
            }
        }
        
        if (successor == null) {
            logger.fatal("FATAL ERROR! Can't find successor for server '" + server_address + "'!");
            System.exit(-1);
        }
        
        return successor;
    }
    
    public ServerAddress getSuccessorAddressForServer(ServerAddress server_address) {
        return findSuccessorForServer(server_address, true).server_address;
    }
    
    public ServerAddress getServerForKey(String key) {
        HashValue       key_hash = HashValue.hashKey(key);
        ServerAddress   address = null;
        
        for (MetaDataItem item: this.records.values()) {
            if (key_hash.isInRange(item.begin_hash, item.end_hash)) {
                address = item.server_address;
                break;
            }
        }
        
        if (address == null) {
            logger.fatal("FATAL ERROR! Key '" + key + "' is not hashed!");
            System.exit(-1);
        }
        
        return address;
    }
    
    public HashValue[] getHashRangeForServer(ServerAddress address) {
        MetaDataItem    server = findServer(address);
        
        HashValue[] range = new HashValue[2];
        range[0] = server.begin_hash;
        range[1] = server.end_hash;
        
        return range;
    }
    
    public static byte[] marshal(ServiceMetaData metadata) {
        Marshaller  marshaller = new Marshaller();
        
        marshaller.marshalInt(metadata.records.size());
        
        for (MetaDataItem item: metadata.records.values()) {
            marshaller.marshalString(item.server_address.toString());
            marshaller.marshalBytes(item.begin_hash.getData());
            marshaller.marshalBytes(item.end_hash.getData());
        }
        
        return marshaller.getBytes();
    }
    
    public static ServiceMetaData unmarshal(byte[] data) throws ParseException {
        Unmarshaller unmarshaller = new Unmarshaller(data);
        
        int count = unmarshaller.unmarshalInt();
        
        Map<ServerAddress, MetaDataItem>    records = new HashMap<ServerAddress, MetaDataItem>(count);
        for (int i = 0; i < count; ++i) {
            MetaDataItem item = new MetaDataItem(new ServerAddress(unmarshaller.unmarshalString()),
                    new HashValue(unmarshaller.unmarshalBytes(HashValue.VALUE_SIZE)),
                    new HashValue(unmarshaller.unmarshalBytes(HashValue.VALUE_SIZE))
            );
            records.put(item.server_address, item);
        }
        
        return new ServiceMetaData(records);
    }
    
    private static class MetaDataItem {
        ServerAddress   server_address;
        HashValue       begin_hash;
        HashValue       end_hash;

        public MetaDataItem(ServerAddress server_address, HashValue begin_hash, HashValue end_hash) {
            this.server_address = server_address;
            this.begin_hash = begin_hash;
            this.end_hash = end_hash;
        }
    }
    
    private static class AddressHashPair implements Comparable<AddressHashPair> {
        ServerAddress   server_address;
        HashValue       hash;

        public AddressHashPair(ServerAddress server_address, HashValue hash) {
            this.server_address = server_address;
            this.hash = hash;
        }

        @Override
        public int compareTo(AddressHashPair o) {
            return this.hash.compareTo(o.hash);
        }
    }

    @Override
    public String toString() {
        StringBuilder   builder = new StringBuilder();
        
        for (MetaDataItem item: this.records.values()) {
            builder.append("Address: ").append(item.server_address).append("; ");
            builder.append("Begin: ").append(item.begin_hash).append("; ");
            builder.append("End: ").append(item.end_hash).append("\n");
        }
        builder.deleteCharAt(builder.length() - 1);
        
        return builder.toString();
    }
}
