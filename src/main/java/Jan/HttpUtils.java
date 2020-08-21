package Jan;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import org.apache.commons.io.Charsets;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * http请求工具类
 *
 * @author zhangjw
 * @date 2018-11-15 14:05
 **/
public class HttpUtils {

	private static final String DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko)"
			+ " Chrome/33.0.1750.146 Safari/537.36";

	private static Logger logger = LoggerFactory.getLogger(HttpUtils.class);

	/**
	 * 发送get请求
	 * @param reqUrl
	 * @param params
	 * @return
	 */
	public static String sendGet(String reqUrl, Map<String, String> params) {
		InputStream inputStream = null;
		HttpGet request = new HttpGet();

		try {
			String url = buildUrl(reqUrl, params);
			HttpClient client = new DefaultHttpClient();

			request.setHeader("Accept-Encoding", "gzip");
			request.setURI(new URI(url));

			HttpResponse response = client.execute(request);

			inputStream = response.getEntity().getContent();
			return getJsonStringFromGzip(inputStream);
		} catch (Exception e) {
			logger.error("httpUtils sendGet请求失败，原因为：", e);
		} finally {
			try {
				if (inputStream != null) {
					inputStream.close();
				}
			} catch (Exception e) {
				logger.error("httpUtils sendGet请求失败，原因为：", e);
			}
			request.releaseConnection();
		}
		return null;
	}

	/**
	 * http发送Post请求
	 *
	 * @param reqUrl
	 * @param params
	 * @return
	 */
	public static String sendPost(String reqUrl, Map<String, String> params) {
		try {
			Set<String> set = params.keySet();
			List<NameValuePair> list = new ArrayList<NameValuePair>();
			for (String key : set) {
				list.add(new BasicNameValuePair(key, params.get(key)));
			}

			if (list.size() > 0) {
				HttpClient client = new DefaultHttpClient();
				HttpPost request = new HttpPost(reqUrl);

				request.setHeader("Accept-Encoding", "gzip");
				request.setEntity(new UrlEncodedFormEntity(list, HTTP.UTF_8));
				HttpResponse response = client.execute(request);

				InputStream inputStream = response.getEntity().getContent();
				try {
					return getJsonStringFromGzip(inputStream);
				} finally {
					inputStream.close();
				}
			} else {
				logger.error("httpUtils sendPost请求失败，参数不全，请稍后重试");
			}
		} catch (Exception e) {
			logger.error("httpUtils sendPost请求失败，原因为：", e);
		}

		return null;
	}

	/**
	 * get请求构造url
	 * @param reqUrl
	 * @param params
	 * @return
	 */
	public static String buildUrl(String reqUrl, Map<String, String> params) {
		StringBuilder query = new StringBuilder();
		Set<String> set = params.keySet();
		for (String key : set) {
			query.append(String.format("%s=%s&", key, params.get(key)));
		}

		return reqUrl + "?" + query.toString();
	}

	/**
	 * 得到inputStream中的String
	 *
	 * @param inputStream
	 * @return
	 */
	public static String getJsonStringFromGzip(InputStream inputStream) {
		String jsonString = null;
		try {
			BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
			bufferedInputStream.mark(2);

			//取前两个字节
			byte[] header = new byte[2];
			int result = bufferedInputStream.read(header);
			bufferedInputStream.reset();

			//判断是否为GZIP格式
			int headerData = getSort(header);
			//Gzip流的前两个字节是0x1f8b
			if (result != -1 && headerData == 0x1f8b) {
				inputStream = new GZIPInputStream(bufferedInputStream);
			} else {
				inputStream = bufferedInputStream;
			}

			InputStreamReader reader = new InputStreamReader(inputStream, "utf-8");
			char[] data = new char[100];
			int readSize;
			StringBuffer stringBuffer = new StringBuffer();
			while ((readSize = reader.read(data)) > 0) {
				stringBuffer.append(data, 0, readSize);
			}
			jsonString = stringBuffer.toString();
			bufferedInputStream.close();
			reader.close();
		} catch (IOException e) {
			logger.error("httpUtils getJsonStringFromGzip请求失败，原因为：", e);
		}

		return jsonString;
	}

	private static int getSort(byte[] data) {
		return (data[0] << 8) | data[1] & 0xFF;
	}

	/**
	 * 用ssl发送请求
	 * @param url
	 * @param data
	 * @param certPath
	 * @param mchId
	 * @return
	 */
	public static String postSSL(String url, String data, String certPath, String mchId) {
		HttpsURLConnection conn = null;
		OutputStream out = null;
		InputStream inputStream = null;
		BufferedReader reader = null;

		try {
			KeyStore clientStore = KeyStore.getInstance("PKCS12");
			clientStore.load(new FileInputStream(certPath), mchId.toCharArray());
			KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
			kmf.init(clientStore, mchId.toCharArray());
			KeyManager[] kms = kmf.getKeyManagers();
			SSLContext sslContext = SSLContext.getInstance("TLSv1");

			sslContext.init(kms, null, new SecureRandom());
			HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
			URL _url = new URL(url);
			conn = (HttpsURLConnection) _url.openConnection();

			conn.setConnectTimeout(25000);
			conn.setReadTimeout(25000);
			conn.setRequestMethod("POST");
			conn.setDoOutput(true);
			conn.setDoInput(true);

			conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
			conn.setRequestProperty("User-Agent", DEFAULT_USER_AGENT);
			conn.connect();

			out = conn.getOutputStream();
			out.write(data.getBytes(Charsets.toCharset("utf-8")));
			out.flush();

			inputStream = conn.getInputStream();
			reader = new BufferedReader(new InputStreamReader(inputStream, Charsets.toCharset("utf-8")));
			StringBuilder sb = new StringBuilder();
			String line = null;
			while ((line = reader.readLine()) != null) {
				sb.append(line).append("\n");
			}
			return sb.toString();
		} catch (Exception e) {
			logger.error("HttpUtils.postSSL请求失败，原因为：", e);
			return null;
		} finally {
			try {
				out.close();
				reader.close();
				inputStream.close();
			} catch (Exception e) {
				logger.error("HttpUtils.postSSL请求失败，原因为：", e);
			}
			if (conn != null) {
				conn.disconnect();
			}
		}
	}

	/**
	 * http post请求json数据
	 *
	 * @param reqUrl
	 * @param params
	 * @return
	 * @throws IOException
	 */
	public static String sendPostByObjectParam(String reqUrl, Map<String, Object> params) throws Exception {
		Set<String> set = params.keySet();
		List<NameValuePair> list = new ArrayList<NameValuePair>();
		for (String key : set) {
			if (null != params.get(key)) {
				list.add(new BasicNameValuePair(key, params.get(key).toString()));
			}
		}

		if (list.size() > 0) {
			HttpClient client = new DefaultHttpClient();
			HttpPost request = new HttpPost(reqUrl);

			request.setHeader("Accept-Encoding", "gzip");
			request.setEntity(new UrlEncodedFormEntity(list, HTTP.UTF_8));

			HttpResponse response = client.execute(request);

			InputStream inputStream = response.getEntity().getContent();
			try {
				String result = getJsonStringFromGzip(inputStream);
				return result;
			} finally {
				inputStream.close();
			}

		} else {
			logger.error("参数不全，请稍后重试");
		}

		return null;
	}

	/**
	 * 另一种发送post请求的方法
	 * @param url
	 * @param param
	 * @return
	 */
	public static String sendPost(String url, String param) {
		PrintWriter out = null;
		BufferedReader in = null;
		String result = "";
		try {
			URL realUrl = new URL(url);
			// 打开和URL之间的连接
			URLConnection conn = realUrl.openConnection();
			// 设置通用的请求属性
			conn.setRequestProperty("accept", "*/*");
			conn.setRequestProperty("connection", "Keep-Alive");
			conn.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
			// 发送POST请求必须设置如下两行
			conn.setDoOutput(true);
			conn.setDoInput(true);
			// 获取URLConnection对象对应的输出流
			out = new PrintWriter(conn.getOutputStream());
			// 发送请求参数
			out.print(param);
			// flush输出流的缓冲
			out.flush();
			// 定义BufferedReader输入流来读取URL的响应
			in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String line;
			while ((line = in.readLine()) != null) {
				result += line;
			}
		} catch (Exception e) {
			logger.error("发送 POST请求出现异常，原因为:", e);
		}
		//使用finally块来关闭输出流、输入流
		finally {
			try {
				if (out != null) {
					out.close();
				}
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
				logger.error("发送 POST请求出现异常，原因为:", ex);
			}
		}
		return result;
	}
}
