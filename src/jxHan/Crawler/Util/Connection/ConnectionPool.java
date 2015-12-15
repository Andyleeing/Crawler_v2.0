package jxHan.Crawler.Util.Connection;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.UnknownHostException;

import javax.net.ssl.SSLException;

import jxHan.Crawler.Util.LoadProperties;
import jxHan.Crawler.Util.Log.ExceptionHandler;

import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.protocol.ExecutionContext;
import org.apache.http.protocol.HttpContext;

public class ConnectionPool {
	private static DefaultHttpClient client;
	private static PoolingClientConnectionManager cm;
	SchemeRegistry schemeRegistry = new SchemeRegistry();
	public static DefaultHttpClient getDefaultHttpClient() {
		if (cm != null && client != null)
			return client;
		SchemeRegistry schemeRegistry = new SchemeRegistry();
		schemeRegistry.register(new Scheme("http", 80, PlainSocketFactory
				.getSocketFactory()));
		schemeRegistry.register(new Scheme("https", 443, SSLSocketFactory
				.getSocketFactory()));
		cm = new PoolingClientConnectionManager(schemeRegistry);
		int MaxTotal = Integer.parseInt(LoadProperties.getConnPoolMgrPro()
				.getProperty("MaxTotal"));
		cm.setMaxTotal(MaxTotal);
		int DefaultMaxPerRoute = Integer.parseInt(LoadProperties
				.getConnPoolMgrPro().getProperty("DefaultMaxPerRoute"));
		cm.setDefaultMaxPerRoute(DefaultMaxPerRoute);
		HttpHost localhost = new HttpHost("locahost", 80);
		int localhostMaxPerRoute = Integer.parseInt(LoadProperties
				.getConnPoolMgrPro().getProperty("localhostMaxPerRoute"));
		cm.setMaxPerRoute(new HttpRoute(localhost), localhostMaxPerRoute);// 对本机80端口的socket配置连接上限
		client = new DefaultHttpClient(cm);
		int socketTimeout = Integer.parseInt(LoadProperties
				.getConnPoolMgrPro().getProperty("socketTimeout"));
		int connectionTimeout = Integer.parseInt(LoadProperties
				.getConnPoolMgrPro().getProperty("connectionTimeout"));
		client.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT,
				socketTimeout);
		client.getParams().setParameter(
				CoreConnectionPNames.CONNECTION_TIMEOUT, connectionTimeout);
		client.getParams()
				.setParameter(CoreConnectionPNames.TCP_NODELAY, false);
		client.getParams().setParameter(
				CoreConnectionPNames.SOCKET_BUFFER_SIZE, 1024 * 1024);
		HttpRequestRetryHandler myRetryHandler = new HttpRequestRetryHandler() {

			@Override
			public boolean retryRequest(IOException exception,
					int executionCount, HttpContext context) {
				int retryTime = Integer.parseInt(LoadProperties
						.getConnPoolMgrPro().getProperty("retryTime"));
				if (executionCount >= retryTime) {
					ExceptionHandler.log("executionCount >= retryTime", exception);
					return false;
				}
				if (exception instanceof InterruptedIOException) {
					// Timeout
					ExceptionHandler.log("Timeout", exception);
					return false;
				}
				if (exception instanceof UnknownHostException) {
					// Unknown host
					ExceptionHandler.log("Unknown host", exception);
					return false;
				}
				if (exception instanceof ConnectException) {
					// Connection refused
					ExceptionHandler.log("Connection refused", exception);
					return false;
				}
				if (exception instanceof SSLException) {
					// SSL handshake exception
					ExceptionHandler.log("SSL handshake exception", exception);
					return false;
				}
				HttpRequest request = (HttpRequest) context
						.getAttribute(ExecutionContext.HTTP_REQUEST);
				boolean idempotent = !(request instanceof HttpEntityEnclosingRequest);
				if (idempotent) {
					// Retry if the request is considered idempotent
					return true;
				}
				return false;
			}
		};
		client.setHttpRequestRetryHandler(myRetryHandler);
		return client;
	}

	public static PoolingClientConnectionManager getPoolingClientConnectionManager() {
		return cm;
	}

}
