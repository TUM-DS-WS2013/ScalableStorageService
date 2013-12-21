package common.messages;

import common.topology.ServiceMetaData;
import java.text.ParseException;
import server.DataTransferRequest;
import server.KeyValuePacket;

/**
 *
 * @author Danila Klimenko
 */
public class ControlMessage {
    public enum ControlType {
        SUCCESS,
        FAILURE,
        INIT,
        START,
        STOP,
        SHUTDOWN,
        LOCK_WRITE,
        UNLOCK_WRITE,
        MOVE_DATA,
        DELETE_DATA,
        UPDATE,
        TRANSFER
    }
    
    private final ControlType           type;
    private final String                description;
    private final ServiceMetaData       meta_data;
    private final DataTransferRequest   data_transfer_request;
    private final KeyValuePacket        key_value_packet;
    
    public ControlMessage(ControlType type) {
        this.type = type;
        this.description = null;
        this.meta_data = null;
        this.data_transfer_request = null;
        this.key_value_packet = null;
        
        this.verifyMessageConsistency();
    }
    
    public ControlMessage(ControlType type, String description) {
        this.type = type;
        this.description = description;
        this.meta_data = null;
        this.data_transfer_request = null;
        this.key_value_packet = null;
        
        this.verifyMessageConsistency();
    }
    
    public ControlMessage(ControlType type, String description, ServiceMetaData meta_data) {
        this.type = type;
        this.description = description;
        this.meta_data = meta_data;
        this.data_transfer_request = null;
        this.key_value_packet = null;
        
        this.verifyMessageConsistency();
    }
    
    public ControlMessage(ControlType type, ServiceMetaData meta_data) {
        this.type = type;
        this.description = null;
        this.meta_data = meta_data;
        this.data_transfer_request = null;
        this.key_value_packet = null;
        
        this.verifyMessageConsistency();
    }
    
    public ControlMessage(ControlType type, DataTransferRequest dt_request) {
        this.type = type;
        this.description = null;
        this.meta_data = null;
        this.data_transfer_request = dt_request;
        this.key_value_packet = null;
        
        this.verifyMessageConsistency();
    }
    
    public ControlMessage(ControlType type, KeyValuePacket key_value_pairs) {
        this.type = type;
        this.description = null;
        this.meta_data = null;
        this.data_transfer_request = null;
        this.key_value_packet = key_value_pairs;
        
        this.verifyMessageConsistency();
    }

    public ControlMessage(ControlType type, String description, ServiceMetaData meta_data,
            DataTransferRequest data_transfer_request, KeyValuePacket key_value_packet) {
        this.type = type;
        this.description = description;
        this.meta_data = meta_data;
        this.data_transfer_request = data_transfer_request;
        this.key_value_packet = key_value_packet;
        
        this.verifyMessageConsistency();
    }
    
    private void verifyMessageConsistency() {
        if ((this.type == null) || (ControlMessage.messageTypeHasDescription(this.type) && this.description == null) ||
                (ControlMessage.messageTypeHasMetadata(this.type) && this.meta_data == null) ||
                (ControlMessage.messageTypeHasDataTransferRequest(this.type) && this.data_transfer_request == null) ||
                (ControlMessage.messageTypeHasKeyValuePacket(this.type) && this.key_value_packet == null)) {
            throw new IllegalArgumentException("ControlMessage is illegaly constructed.");
        }
    }
    
    public ControlType getType() {
        return this.type;
    }
    public String getDescription() {
        return this.description;
    }
    public ServiceMetaData getMetaData() {
        return this.meta_data;
    }
    public DataTransferRequest getDataTransferRequest() {
        return this.data_transfer_request;
    }
    public KeyValuePacket getKeyValuePacket() {
        return this.key_value_packet;
    }
    
    public static boolean isControlMessage(byte[] data) {
        boolean result;
        
        try {
            Unmarshaller unmarshaller = new Unmarshaller(data);
            result = unmarshaller.unmarshalByte() == CONTROLMESSAGE_SIGNATURE;
        } catch (ParseException ex) {
            result = false;
        }
        
        return result;
    }
    
    private static boolean messageTypeHasDescription(ControlType type) {
        return (type == ControlType.INIT) || (type == ControlType.FAILURE);
    }
    private static boolean messageTypeHasMetadata(ControlType type) {
        return (type == ControlType.INIT) || (type == ControlType.UPDATE);
    }
    private static boolean messageTypeHasDataTransferRequest(ControlType type) {
        return (type == ControlType.MOVE_DATA) || (type == ControlType.DELETE_DATA);
    }
    private static boolean messageTypeHasKeyValuePacket(ControlType type) {
        return (type == ControlType.TRANSFER);
    }
    
    
    private static final byte   CONTROLMESSAGE_SIGNATURE = (byte)0xB1;
    
    public static byte[] marshal(ControlMessage ctrlmsg) {
        Marshaller marshaller = new Marshaller();
        
        ControlType    type = ctrlmsg.getType();
        marshaller.marshalByte(CONTROLMESSAGE_SIGNATURE);
        marshaller.marshalString(type.name());
        
        if (ControlMessage.messageTypeHasDescription(type)) {
            marshaller.marshalString(ctrlmsg.getDescription());
        }
        if (ControlMessage.messageTypeHasMetadata(type)) {
            byte[]  bytes = ServiceMetaData.marshal(ctrlmsg.getMetaData());
            marshaller.marshalInt(bytes.length);
            marshaller.marshalBytes(bytes);
        }
        if (ControlMessage.messageTypeHasDataTransferRequest(type)) {
            byte[]  bytes = DataTransferRequest.marshal(ctrlmsg.getDataTransferRequest());
            marshaller.marshalInt(bytes.length);
            marshaller.marshalBytes(bytes);
        }
        if (ControlMessage.messageTypeHasKeyValuePacket(type)) {
            byte[]  bytes = KeyValuePacket.marshal(ctrlmsg.getKeyValuePacket());
            marshaller.marshalInt(bytes.length);
            marshaller.marshalBytes(bytes);
        }
        
        return marshaller.getBytes();
    }
    
    public static ControlMessage unmarshal(byte[] data) throws ParseException {
        Unmarshaller unmarshaller = new Unmarshaller(data);
        
        if (unmarshaller.unmarshalByte() != CONTROLMESSAGE_SIGNATURE) {
            throw new ParseException("Received message is not a valid ControlMessage.", 0);
        }
        
        ControlType  type;
        try {
            type = ControlType.valueOf(unmarshaller.unmarshalString());
        } catch (IllegalArgumentException ex) {
            throw new ParseException("Invalid message type.", unmarshaller.position());
        }
        
        String description = null;
        ServiceMetaData meta_data = null;
        DataTransferRequest dt_request = null;
        KeyValuePacket kv_packet = null;
        if (ControlMessage.messageTypeHasDescription(type)) {
            description = unmarshaller.unmarshalString();
        }
        if (ControlMessage.messageTypeHasMetadata(type)) {
            int size = unmarshaller.unmarshalInt();
            meta_data = ServiceMetaData.unmarshal(unmarshaller.unmarshalBytes(size));
        }
        if (ControlMessage.messageTypeHasDataTransferRequest(type)) {
            int size = unmarshaller.unmarshalInt();
            dt_request = DataTransferRequest.unmarshal(unmarshaller.unmarshalBytes(size));
        }
        if (ControlMessage.messageTypeHasKeyValuePacket(type)) {
            int size = unmarshaller.unmarshalInt();
            kv_packet = KeyValuePacket.unmarshal(unmarshaller.unmarshalBytes(size));
        }
        
        return new ControlMessage(type, description, meta_data, dt_request, kv_packet);
    }
}
