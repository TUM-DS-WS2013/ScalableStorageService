package common.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Special class representing low-level message format for client-server interaction
 * @author Danila Klimenko
 */
public class NetworkMessage {
    private static final int    MAX_MESSAGE_SIZE = 128 * 1024;
    private static final int    SIZEOF_LENGTH = 4;
    
    private final int       length;
    private final byte[]    data;
    
    /**
     * Main constructor from a byte array.
     * @param data Array of bytes to be sent
     * @throws IOException Thrown if data size exceeds the limit
     */
    public NetworkMessage(byte[] data) throws IOException {
        if (data.length > MAX_MESSAGE_SIZE) {
            throw new IOException("Message size limit exceeded.");
        }
        
        this.data = data;
        this.length = data.length;
    }
    
    /**
     * Getter method for the contents of the message
     * @return Message contents as an array of bytes
     */
    public byte[] getData() {
        return this.data;
    }
    
    /**
     * Write the message to the given OutputStream.
     * @param os Output stream to write the message to
     * @throws IOException Thrown if OutputStream malfunctions
     */
    public void writeTo(OutputStream os) throws IOException {
        ByteBuffer bbuf = ByteBuffer.allocate(SIZEOF_LENGTH + this.length);
        
        bbuf.putInt(this.length);
        bbuf.put(this.data);
        
        os.write(bbuf.array());
        os.flush();
    }
    
    /**
     * Static method reading a message from the given InputStream.
     * @param is Input stream to read the message from
     * @return A valid NetworkMessage instance
     * @throws IOException Thrown if InputStream malfunctions
     */
    public static NetworkMessage readFrom(InputStream is) throws IOException {
        DataInputStream dis = new DataInputStream(is);
        
        int length = dis.readInt();
        if (length > MAX_MESSAGE_SIZE) {
            throw new IOException("Message size limit exceeded.");
        }
        
        byte[] data = new byte[length];
        
        int read_bytes = 0;
        int total_read_bytes = 0;
        
        while (total_read_bytes < length) {
            read_bytes = dis.read(data, total_read_bytes, length - total_read_bytes);
            
            if (read_bytes > 0) {
                total_read_bytes += read_bytes;
            } else {
                break;
            }
        }
        
        if (total_read_bytes != length) {
            throw new IOException("Message is incomplete: expected length = " +
                                    length + "; available = " + total_read_bytes + ".");
        }
        
        return new NetworkMessage(data);
    }
}
