package common.messages;

import java.nio.ByteBuffer;
import java.text.ParseException;

/**
 *
 * @author Danila Klimenko
 */
public class Unmarshaller {
    private static final int    SIZEOF_INT = 4;
    
    private final ByteBuffer    bbuf;

    public Unmarshaller(byte[] data) {
        this.bbuf = ByteBuffer.wrap(data);
    }
    
    public int position() {
        return this.bbuf.position();
    }
    
    public byte unmarshalByte() throws ParseException {
        if (this.bbuf.remaining() < 1) {
            throw new ParseException("unmarshalBytes(): Buffer is incomplete: expected length = " + 1 +
                    "; available = " + this.bbuf.remaining() + ".", this.bbuf.position());
        }
        
        return this.bbuf.get();
    }
    
    public int unmarshalInt() throws ParseException {
        if (this.bbuf.remaining() < SIZEOF_INT) {
            throw new ParseException("unmarshalBytes(): Buffer is incomplete: expected length = " + SIZEOF_INT +
                    "; available = " + this.bbuf.remaining() + ".", this.bbuf.position());
        }
        
        return this.bbuf.getInt();
    }
    
    public byte[] unmarshalBytes(int count) throws ParseException {
        byte[] bytes = null;
        
        if (count > 0) {
            if (this.bbuf.remaining() < count) {
                throw new ParseException("unmarshalBytes(): Buffer is incomplete: expected length = " + count +
                        "; available = " + this.bbuf.remaining() + ".", this.bbuf.position());
            }
            
            bytes = new byte[count];
            this.bbuf.get(bytes);
        }
        
        return bytes;
    }
    
    public String unmarshalString() throws ParseException {
        String str = null;
        
        if (this.bbuf.remaining() < SIZEOF_INT) {
            throw new ParseException("unmarshalString(): Buffer is incomplete: expected length = " + SIZEOF_INT +
                    "; available = " + this.bbuf.remaining() + ".", this.bbuf.position());
        }
        
        int len = this.bbuf.getInt();
        
        if (len > 0) {
            if (this.bbuf.remaining() < len) {
                throw new ParseException("String is incomplete: expected length = " + len +
                        "; available = " + this.bbuf.remaining() + ".", this.bbuf.position());
            }
            
            byte[]  str_data = new byte[len];
            this.bbuf.get(str_data);
            
            str = new String(str_data);
        }
        
        return str;
    }
}
