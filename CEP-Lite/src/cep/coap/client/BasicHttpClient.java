package cep.coap.client;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;
import org.json.JSONObject;





public class BasicHttpClient {
	private static String SERVER_ADDRESS = "";
	
	public BasicHttpClient(String serverAddress)
	{
		SERVER_ADDRESS=serverAddress;
		
	}
    public void sendMessage(String payload)
	{
    	StringEntity se;
		try{
			DefaultHttpClient httpClient=new DefaultHttpClient();
			HttpPost httppostreq;
			httppostreq = new HttpPost(SERVER_ADDRESS);
			
				se = new StringEntity(payload);
			
			se.setContentType("application/json;charset=UTF-8");
	        se.setContentEncoding(new BasicHeader(HTTP.CONTENT_TYPE, "application/json;charset=UTF-8"));
	        httppostreq.setEntity(se);
	        httppostreq.addHeader("data",payload);
	        HttpResponse response=httpClient.execute(httppostreq);
	        //Log.d("MESSAGE",payload);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	/*HttpURLConnection urlConnection=null;
		try {
			URL urlToRequest = new URL(SERVER_ADDRESS);
			urlConnection =(HttpURLConnection) urlToRequest.openConnection();
		    	urlConnection.setDoOutput(true);
		    	urlConnection.setRequestMethod("POST");
		    	urlConnection.setRequestProperty("Content-Type","application/json;charset=UTF-8");
		    	urlConnection.setRequestProperty("Content-Length", "" + 
		                Integer.toString(payload.length()));
		    	//urlConnection.setRequestProperty("Content-Language", "en-US");  
		 			
		    	urlConnection.setUseCaches (false);
		    	urlConnection.setDoInput(true);
		    	urlConnection.setDoOutput(true);
		    	DataOutputStream wr = new DataOutputStream (
		                 urlConnection.getOutputStream ());
		        wr.writeBytes (payload);
		        wr.flush ();
		        wr.close ();
		        InputStream is = urlConnection.getInputStream();
		        BufferedReader rd = new BufferedReader(new InputStreamReader(is));
		        String line;
		        StringBuffer response = new StringBuffer(); 
		        while((line = rd.readLine()) != null) {
		          response.append(line);
		          response.append('\r');
		        }
		        rd.close();
		        Log.d("Response",response.toString());
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally
		{
			urlConnection.disconnect();
		}*/
    	
	}

}
