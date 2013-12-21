package server;

import common.messages.Marshaller;
import common.messages.Unmarshaller;
import common.topology.HashValue;
import common.topology.ServerAddress;
import java.text.ParseException;

/**
 *
 * @author Danila Klimenko
 */
public class DataTransferRequest {
    private final HashValue     range_begin;
    private final HashValue     range_end;
    private final ServerAddress target;

    public DataTransferRequest(HashValue range_begin, HashValue range_end, ServerAddress target) {
        this.range_begin = range_begin;
        this.range_end = range_end;
        this.target = target;
    }

    public HashValue getRangeBegin() {
        return this.range_begin;
    }

    public HashValue getRangeEnd() {
        return this.range_end;
    }
    
    public ServerAddress getTarget() {
        return this.target;
    }
    
    public static byte[] marshal(DataTransferRequest dt_request) {
        Marshaller  marshaller = new Marshaller();
        
        marshaller.marshalBytes(dt_request.range_begin.getData());
        marshaller.marshalBytes(dt_request.range_end.getData());
        marshaller.marshalString(dt_request.target.toString());
        
        return marshaller.getBytes();
    }
    
    public static DataTransferRequest unmarshal(byte[] data) throws ParseException {
        Unmarshaller unmarshaller = new Unmarshaller(data);
        
        HashValue       range_begin = new HashValue(unmarshaller.unmarshalBytes(HashValue.VALUE_SIZE));
        HashValue       range_end = new HashValue(unmarshaller.unmarshalBytes(HashValue.VALUE_SIZE));
        ServerAddress   target = new ServerAddress(unmarshaller.unmarshalString());
        
        return new DataTransferRequest(range_begin, range_end, target);
    }
}
