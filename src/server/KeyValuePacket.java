package server;

import common.messages.Marshaller;
import common.messages.Unmarshaller;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author Danila Klimenko
 */
public class KeyValuePacket implements Iterable<KeyValuePacket.KeyValuePair> {
    private final List<KeyValuePair>  kv_pairs;

    public KeyValuePacket() {
        this.kv_pairs = new ArrayList<KeyValuePair>();
    }

    private KeyValuePacket(List<KeyValuePair> kv_pairs) {
        this.kv_pairs = kv_pairs;
    }
    
    public void addKeyValuePair(String key, String value) {
        this.kv_pairs.add(new KeyValuePair(key, value));
    }
    
    public boolean isEmpty() {
        return this.kv_pairs.isEmpty();
    }

    @Override
    public Iterator<KeyValuePair> iterator() {
        return this.kv_pairs.iterator();
    }
    
    public List<KeyValuePacket> splitOnMarshaledSizeLimit(int size_limit) {
        List<KeyValuePacket> packets = new ArrayList<KeyValuePacket>();
        
        KeyValuePacket  sub_packet = new KeyValuePacket();
        int             sub_packet_size = 4;
        Marshaller      marshaller = new Marshaller();
        
        for (KeyValuePair kv_pair : this.kv_pairs) {
            marshaller.marshalString(kv_pair.key);
            marshaller.marshalString(kv_pair.value);
            
            if (sub_packet_size + marshaller.size() >= size_limit) {
                packets.add(sub_packet);
                sub_packet = new KeyValuePacket();
                sub_packet_size = 4;
            }
            
            sub_packet.addKeyValuePair(kv_pair.key, kv_pair.value);
            sub_packet_size += marshaller.size();
            marshaller.reset();
        }
        packets.add(sub_packet);
        
        return packets;
    }
    
    public static byte[] marshal(KeyValuePacket packet) {
        Marshaller  marshaller = new Marshaller();
        
        marshaller.marshalInt(packet.kv_pairs.size());
        for (KeyValuePair kv_pair : packet.kv_pairs) {
            marshaller.marshalString(kv_pair.key);
            marshaller.marshalString(kv_pair.value);
        }
        
        return marshaller.getBytes();
    }
    
    public static KeyValuePacket unmarshal(byte[] data) throws ParseException {
        Unmarshaller unmarshaller = new Unmarshaller(data);
        
        int count = unmarshaller.unmarshalInt();
        
        List<KeyValuePair>  kv_pairs = new ArrayList<KeyValuePair>(count);
        for (int i = 0; i < count; ++i) {
            kv_pairs.add(new KeyValuePair(unmarshaller.unmarshalString(), unmarshaller.unmarshalString()));
        }
        
        return new KeyValuePacket(kv_pairs);
    }
    
    public static class KeyValuePair {
        String   key;
        String   value;

        KeyValuePair(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }
}
