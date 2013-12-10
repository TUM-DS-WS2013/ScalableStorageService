package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import common.messages.KVMessage;
import common.messages.KVMessageRaw;
import common.messages.NetworkMessage;
import java.text.ParseException;

public class KVStore implements KVCommInterface {

    private Socket objSocketClient;
    private InputStream objSocketInputStream;
    private OutputStream objSocketOutPutStream;
    private final String strServerAdress;
    private final int nServerPort;

    /**
     * Get the IP address of server.
     *
     * @return the IP address of server to whom socket is connected.
     */
    public String GetServerIP() {
        return strServerAdress;
    }

    /**
     * Get the port number of server.
     *
     * @return port number to whom the socket is connected.
     */
    public int GetServerPort() {
        return nServerPort;
    }

    public KVStore() {
        strServerAdress = "";
        nServerPort = -1;
        objSocketClient = null;
        objSocketInputStream = null;
        objSocketOutPutStream = null;
    }

    /**
     * Initialize KVStore with address and port of KVServer
     *
     * @param address the address of the KVServer
     * @param port the port of the KVServer
     */
    public KVStore(String address, int port) {
        strServerAdress = address;
        nServerPort = port;
    }

    @Override
    public void connect() throws Exception {

        if (objSocketClient == null && !strServerAdress.isEmpty() && nServerPort != -1) {
            objSocketClient = new Socket(strServerAdress, nServerPort);
            objSocketInputStream = objSocketClient.getInputStream();
            objSocketOutPutStream = objSocketClient.getOutputStream();
        }
    }

    @Override
    public void disconnect() {
        try {
            if (objSocketInputStream != null) {
                objSocketInputStream.close();
            }
            if (objSocketOutPutStream != null) {
                objSocketOutPutStream.close();
            }
            if (objSocketClient != null) {
                objSocketClient.close();
            }
        } catch (IOException ex) {
        }

        objSocketInputStream = null;
        objSocketOutPutStream = null;
        objSocketClient = null;
    }

    @Override
    public KVMessage put(String key, String value) throws Exception {
        KVMessage kvmsg = new KVMessageRaw(KVMessage.StatusType.PUT, key, value);
        
        return this.kvRequest(kvmsg);
    }

    @Override
    public KVMessage get(String key) throws Exception {
        KVMessage   kvmsg = new KVMessageRaw(KVMessage.StatusType.GET, key, null);
        
        return this.kvRequest(kvmsg);
    }
    
    public KVMessage kvRequest(KVMessage kv_out) throws IOException {
        NetworkMessage netmsg = new NetworkMessage(KVMessageRaw.marshal(kv_out));
        netmsg.writeTo(objSocketOutPutStream);
        
        KVMessage kv_in;
        netmsg = NetworkMessage.readFrom(objSocketInputStream);
        try {
            kv_in = KVMessageRaw.unmarshal(netmsg.getData());
            // TODO: remove the next line!
            //System.out.println(kv_in.getStatus() + " " + kv_in.getKey() + " " + kv_in.getValue());
        } catch (ParseException e) {
            kv_in = null;
            String error_message = new String(netmsg.getData());
            // TODO: remove the next line!
            //System.out.println(error_message);
            throw new IOException(error_message);
        }
        
        return kv_in;
    }
}
