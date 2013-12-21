package common.messages;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

/**
 *
 * @author Danila Klimenko
 */
public class Marshaller {
    private final ByteArrayOutputStream baos;
    
    public Marshaller() {
        this.baos = new ByteArrayOutputStream();
    }
    public Marshaller(int size) {
        this.baos = new ByteArrayOutputStream(size);
    }
    
    public int size() {
        return this.baos.size();
    }
    
    public void reset() {
        this.baos.reset();
    }
    
    public byte[] getBytes() {
        return this.baos.toByteArray();
    }
    
    public void marshalByte(byte val) {
        this.baos.write(val);
    }
    
    public void marshalInt(int val) {
        byte[] bytes = ByteBuffer.allocate(4).putInt(val).array();
        this.baos.write(bytes, 0, bytes.length);
    }
    
    public void marshalBytes(byte[] bytes) {
        if (bytes != null) {
            this.baos.write(bytes, 0, bytes.length);
        }
    }
    
    public void marshalString(String str) {
        ByteBuffer bbuf;
        
        if (str != null) {
            byte[] str_data = str.getBytes();
            bbuf = ByteBuffer.allocate(4 + str_data.length);
            bbuf.putInt(str_data.length);
            bbuf.put(str_data);
        } else {
            bbuf = ByteBuffer.allocate(4);
            bbuf.putInt(0);
        }
        
        this.baos.write(bbuf.array(), 0, bbuf.array().length);
    }
}
