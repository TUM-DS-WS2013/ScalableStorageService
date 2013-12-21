package common.messages;

import common.topology.ServiceMetaData;
import java.text.ParseException;

/**
 * KVMessage implementation based on pure byte streams
 * @author Danila Klimenko
 */
public class KVMessageRaw implements KVMessage {
    private final StatusType        type;
    private final String            key;
    private final String            value;
    private final ServiceMetaData   meta_data;
    
    /**
     * Main constructor for the class. Used for a known key-value pair.
     * @param type Message type
     * @param key The key
     * @param value The value
     */
    public KVMessageRaw(StatusType type, String key, String value) {
        if (type == null) {
            throw new IllegalArgumentException("KVMessageRaw(): type may not be null.");
        }
        this.type = type;
        this.key = key;
        this.value = value;
        this.meta_data = null;
    }
    
    public KVMessageRaw(StatusType type, String key, String value, ServiceMetaData meta_data) {
        if (type == null) {
            throw new IllegalArgumentException("KVMessageRaw(): type may not be null.");
        }
        this.type = type;
        this.key = key;
        this.value = value;
        this.meta_data = meta_data;
    }

    /**
     * Getter method for the key.
     * @return The key
     */
    @Override
    public String getKey() {
        return this.key;
    }

    /**
     * Getter method for the value.
     * @return The value
     */
    @Override
    public String getValue() {
        return this.value;
    }

    /**
     * Getter method for the message type.
     * @return The type of the message
     */
    @Override
    public StatusType getStatus() {
        return this.type;
    }
    
    public ServiceMetaData getMetaData() {
        return this.meta_data;
    }
    
    /**
     * The following are the static methods for marshaling and un-marshaling of
     * the KVMessages, respectively, to and from byte arrays.
     */
    private static final byte   KVMESSAGERAW_SIGNATURE = (byte)0xA1;
    
    public static byte[] marshal(KVMessageRaw kvmsg) {
        Marshaller marshaller = new Marshaller();
        
        marshaller.marshalByte(KVMESSAGERAW_SIGNATURE);
        marshaller.marshalString(kvmsg.getStatus().name());
        
        String  key = kvmsg.getKey() != null ? kvmsg.getKey() : "";
        String  value = kvmsg.getValue()!= null ? kvmsg.getValue() : "";
        marshaller.marshalString(key);
        marshaller.marshalString(value);
        if (kvmsg.getMetaData() != null) {
            byte[]  bytes = ServiceMetaData.marshal(kvmsg.getMetaData());
            marshaller.marshalInt(bytes.length);
            marshaller.marshalBytes(bytes);
        } else {
            marshaller.marshalInt(0);
        }
        
        return marshaller.getBytes();
    }
    
    public static KVMessageRaw unmarshal(byte[] data) throws ParseException {
        Unmarshaller unmarshaller = new Unmarshaller(data);
        
        if (unmarshaller.unmarshalByte() != KVMESSAGERAW_SIGNATURE) {
            throw new ParseException("Received message is not a valid KVMessageRaw.", 0);
        }
        
        StatusType  type;
        try {
            type = StatusType.valueOf(unmarshaller.unmarshalString());
        } catch (IllegalArgumentException ex) {
            throw new ParseException("Invalid message type.", unmarshaller.position());
        }
        
        String  key = unmarshaller.unmarshalString();
        String  value = unmarshaller.unmarshalString();
        
        ServiceMetaData meta_data = null;
        int             meta_data_size = unmarshaller.unmarshalInt();
        if (meta_data_size > 0) {
            meta_data = ServiceMetaData.unmarshal(unmarshaller.unmarshalBytes(meta_data_size));
        }
        
        return new KVMessageRaw(type, key, value, meta_data);
    }
}
