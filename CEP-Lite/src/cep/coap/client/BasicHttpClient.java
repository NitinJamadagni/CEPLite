package cep.coap.client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;

public class BasicHttpClient {
	private static String SERVER_ADDRESS = "";
	DefaultHttpClient httpClient;
    HttpPost httppostreq;
	public BasicHttpClient(String serverAddress)
	{
		SERVER_ADDRESS=serverAddress;
		
	}
    public void sendMessage(String payload)
	{
    	StringEntity se;
		try {
			httpClient = new DefaultHttpClient();
			httppostreq = new HttpPost(SERVER_ADDRESS);
			se = new StringEntity(payload);
			se.setContentType("application/json;charset=UTF-8");
	        se.setContentEncoding(new BasicHeader(HTTP.CONTENT_TYPE, "application/json;charset=UTF-8"));
	        httppostreq.setEntity(se);
	        httpClient.execute(httppostreq);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
		catch (ClientProtocolException e) {
			
			e.printStackTrace();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
        
	}

}
