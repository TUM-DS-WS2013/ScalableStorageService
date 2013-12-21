package common.topology;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import logger.LogSetup;
import org.apache.log4j.Logger;

/**
 *
 * @author Danila Klimenko
 */
public class HashValue implements Comparable<HashValue> {
    private static final Logger     logger = LogSetup.getLogger();
    public static final int         VALUE_SIZE = 16;
    
    private final byte[]        data;
    private final BigInteger    big_int;
    
    public HashValue(byte[] data) {
        this.data = data;
        this.big_int = new BigInteger(data);
    }
    
    public byte[] getData() {
        return this.data;
    }
    
    @Override
    public int compareTo(HashValue rhs) {
        return this.big_int.compareTo(rhs.big_int);
    }
    
    public boolean isInRange(HashValue begin, HashValue end) {
        if (begin.compareTo(end) < 0) {
            return (begin.compareTo(this) < 0) && (this.compareTo(end) <= 0);
        } else {
            return (begin.compareTo(this) < 0) || (this.compareTo(end) <= 0);
        }
    }
    
    public static HashValue hashKey(String key) {
        return HashValue.hashString(key);
    }
    
    public static HashValue hashServerAddress(ServerAddress address) {
        return HashValue.hashString(address.toString());
    }
    
    private static HashValue hashString(String str) {
        HashValue res = null;
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            res = new HashValue(md.digest(str.getBytes()));
        } catch (NoSuchAlgorithmException ex) {
            logger.fatal("FATAL ERROR: Cannot find MD5 hashing algorithm: " + ex.getMessage() + ".");
            System.exit(-1);
        }
        return res;
    }

    @Override
    public String toString() {
        return this.big_int.toString(16);
    }
}
