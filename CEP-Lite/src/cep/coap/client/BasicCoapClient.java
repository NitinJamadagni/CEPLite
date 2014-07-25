package cep.coap.client;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.ws4d.coap.Constants;
import org.ws4d.coap.connection.BasicCoapChannelManager;
import org.ws4d.coap.connection.BasicCoapClientChannel;
import org.ws4d.coap.interfaces.CoapClient;
import org.ws4d.coap.interfaces.CoapClientChannel;
import org.ws4d.coap.interfaces.CoapResponse;
import org.ws4d.coap.messages.BasicCoapRequest;
import org.ws4d.coap.messages.CoapRequestCode;



/**
 * @author Christian Lerche <christian.lerche@uni-rostock.de>
 */

public class BasicCoapClient implements CoapClient {
    //private static final String SERVER_ADDRESS = "10.100.1.118";
    private static String SERVER_ADDRESS = "";
    private static final int PORT = Constants.COAP_DEFAULT_PORT;
    BasicCoapChannelManager channelManager = null;
    BasicCoapClientChannel clientChannel = null;
    BasicCoapRequest coapRequest=null;
    public BasicCoapClient(String serverAddress) throws UnknownHostException {
        //System.out.println("Start CoAP Client");
       
        SERVER_ADDRESS=serverAddress;
         
    	
    	
    }
      
    public void sendMessage(String payload){
    	
    	channelManager = (BasicCoapChannelManager) BasicCoapChannelManager.getInstance();
    	try	{
         	clientChannel = (BasicCoapClientChannel) channelManager.connect(this, InetAddress.getByName(SERVER_ADDRESS), PORT);
         }
         catch(Exception e){
         	e.printStackTrace();
         }
         coapRequest = clientChannel.createRequest(false, CoapRequestCode.PUT);
 		coapRequest.setToken("ABCD".getBytes());
 		coapRequest.setUriHost("123.123.123.121");
 		coapRequest.setUriPort(1234);
 		coapRequest.setUriPath("/sub1/sub2/sub3/");
 		coapRequest.setUriQuery("a=1&b=2&c=3");
 		coapRequest.setProxyUri("http://proxy.org:1234/proxytest");
    	coapRequest.setPayload(payload);
//		coapRequest.setContentType(CoapMediaType.octet_stream);
		clientChannel.sendMessage(coapRequest);
		//System.out.println("Sent Request");
		
    }
    
   	@Override
	public void onConnectionFailed(CoapClientChannel channel, boolean notReachable, boolean resetByServer) {
		System.out.println("Connection Failed");
	}

	@Override
	public void onResponse(CoapClientChannel channel, CoapResponse response) {
		System.out.println("Received response");
	}

	
}

