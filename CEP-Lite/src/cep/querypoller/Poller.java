package cep.querypoller;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.JSONObject;

import cep.core.CEPManager;
import cep.core.OutputHandler;

public class Poller {
	static CEPManager manager=new CEPManager();
	public static void main(String[] args) {
		
			manager.setServerAddress("10.156.14.140",OutputHandler.HTTP);
			ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
			exec.scheduleAtFixedRate(new Runnable() {
			  public void run() {
				  		try {
							String response=sendGet();
							if(response!=""){
								JSONObject object=new JSONObject(response);
								
							}
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}	
			  }
			}, 0,15, TimeUnit.SECONDS);
	}
	
	private static String sendGet() throws Exception {
		 
		String url = "";                //set url here 
		HttpClient client = new DefaultHttpClient();
		HttpGet request = new HttpGet(url);
 
		// add request header
		//request.addHeader();
 
		HttpResponse response = client.execute(request);
 
		System.out.println("\nSending 'GET' request to URL : " + url);
		System.out.println("Response Code : " + 
                       response.getStatusLine().getStatusCode());
 
		BufferedReader rd = new BufferedReader(
                       new InputStreamReader(response.getEntity().getContent()));
 
		StringBuffer result = new StringBuffer();
		String line = "";
		while ((line = rd.readLine()) != null) {
			result.append(line);
		}
 
		return (result.toString());
 
	}

}
