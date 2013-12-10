package common.messages;

import java.nio.ByteBuffer;
import java.text.ParseException;

/**
 * KVMessage implementation based on pure byte streams
 * @author Danila Klimenko
 */
public class KVMessageRaw implements KVMessage {
    StatusType  type;
    String      key;
    String      value;
    
    /**
     * Main constructor for the class. Used for a known key-value pair.
     * @param type Message type
     * @param key The key
     * @param value The value
     */
    public KVMessageRaw(StatusType type, String key, String value) {
        this.type = type;
        this.key = key;
        this.value = value;
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
    
    /**
     * The following are the static methods for marshaling and un-marshaling of
     * the KVMessages, respectively, to and from byte arrays.
     */
    //<editor-fold defaultstate="collapsed" desc="Marshalling routines">
    private static final int SIZEOF_STATUSTYPE = 1;
    private static final int SIZEOF_INT = 4;
    
    /**
     * Converts KVMessage to an array of bytes.
     * @param kvmsg The message to be converted
     * @return Byte array representation of the message
     */
    public static byte[] marshal(KVMessage kvmsg) {
        int     size = SIZEOF_STATUSTYPE;
        String  key = kvmsg.getKey();
        String  value = kvmsg.getValue();
        
        size += SIZEOF_INT + key.getBytes().length;
        size += SIZEOF_INT + ((value != null) ? value.getBytes().length : 0);
        
        ByteBuffer bbuf = ByteBuffer.allocate(size);
        
        marshalType(bbuf, kvmsg.getStatus());
        marshalString(bbuf, kvmsg.getKey());
        marshalString(bbuf, kvmsg.getValue());
        
        return bbuf.array();
    }
    
    /**
     * Parses a KVMessage from a byte array
     * @param data Byte array containing the message
     * @return The parsed KVMessage
     * @throws ParseException Thrown if the given byte array does not represent
     *          a valid KVMessage.
     */
    public static KVMessage unmarshal(byte[] data) throws ParseException {
        ByteBuffer  bbuf = ByteBuffer.wrap(data);
        
        StatusType  type = unmarshalType(bbuf);
        String      key = unmarshalString(bbuf);
        String      value = unmarshalString(bbuf);
        
        return new KVMessageRaw(type, key, value);
    }
    
    private static void marshalType(ByteBuffer bbuf, StatusType stype) {
        bbuf.put((byte)stype.ordinal());
    }
    
    private static void marshalString(ByteBuffer bbuf, String str) {
        if (str != null) {
            byte[] str_data = str.getBytes();
            bbuf.putInt(str_data.length);
            bbuf.put(str_data);
        } else {
            bbuf.putInt(0);
        }
    }
    
    private static StatusType unmarshalType(ByteBuffer bbuf) throws ParseException {
        StatusType stype = null;
        
        if (bbuf.remaining() < SIZEOF_STATUSTYPE) {
            throw new ParseException("Message is empty.", 0);
        }
        
        byte type_data = bbuf.get();
        for (StatusType st: StatusType.values()) {
            if (type_data == st.ordinal()) {
                stype = st;
                break;
            }
        }
        
        if (stype == null) {
            throw new ParseException("Invalid message type.", bbuf.position() - 1);
        }
        
        return stype;
    }
    
    private static String unmarshalString(ByteBuffer bbuf) throws ParseException {
        String str = null;
        
        if (bbuf.remaining() < SIZEOF_INT) {
            throw new ParseException("String length is invalid.", bbuf.position());
        }
        
        int len = bbuf.getInt();
        
        if (len > 0) {
            if (bbuf.remaining() < len) {
                throw new ParseException("String id incomplete: expected length: " + len +
                        "; available: " + bbuf.remaining() + ".", bbuf.position());
            }
            
            byte[]  str_data = new byte[len];
            bbuf.get(str_data);
            
            str = new String(str_data);
        }
        
        return str;
    }
    //</editor-fold>
}
