/**
 * @file    HttpTransport.java
 * @brief   HTTP Transport Support 
 * @author  Doug Anson
 * @version 1.0
 * @see
 *
 * Copyright 2015. ARM Ltd. All rights reserved.
 *
 * Use of this software is restricted by the terms of the license under which this software has been
 * distributed (the "License"). Any use outside the express terms of the License, including wholly
 * unlicensed use, is prohibited. You may not use this software unless you have been expressly granted
 * the right to use it under the License.
 * 
 */

package com.arm.connector.bridge.transport;

import com.arm.connector.bridge.core.BaseClass;
import com.arm.connector.bridge.core.ErrorLogger;
import com.arm.connector.bridge.preferences.PreferenceManager;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.commons.codec.binary.Base64;

/**
 * HTTP Transport Support
 *
 * @author Doug Anson
 */
public class HttpTransport extends BaseClass {
    private int m_last_response_code = 0;
    
    // constructor

    /**
     *
     * @param error_logger
     * @param preference_manager
     */
    public HttpTransport(ErrorLogger error_logger, PreferenceManager preference_manager) {
        super(error_logger, preference_manager);
    }

    // execute GET over http
    /**
     *
     * @param url_str
     * @param username
     * @param password
     * @param data
     * @param content_type
     * @param auth_domain
     * @return
     */
    public String httpGet(String url_str, String username, String password, String data, String content_type, String auth_domain) {
        return this.doHTTP("GET", url_str, username, password, data, content_type, auth_domain, true, false, false, false, null);
    }
    
    /**
     *
     * @param url_str
     * @param api_token
     * @param data
     * @param content_type
     * @param auth_domain
     * @return
     */
    public String httpGetApiTokenAuth(String url_str, String api_token, String data, String content_type, String auth_domain) {
        return this.doHTTP("GET", url_str, null, null, data, content_type, auth_domain, true, false, false, true, api_token);
    }
    
    // execute GET over https
    /**
     *
     * @param url_str
     * @param username
     * @param password
     * @param data
     * @param content_type
     * @param auth_domain
     * @return
     */
    public String httpsGet(String url_str, String username, String password, String data, String content_type, String auth_domain) {
        return this.doHTTP("GET", url_str, username, password, data, content_type, auth_domain, true, false, true, false, null);
    }
    
    /**
     *
     * @param url_str
     * @param api_token
     * @param data
     * @param content_type
     * @param auth_domain
     * @return
     */
    public String httpsGetApiTokenAuth(String url_str, String api_token, String data, String content_type, String auth_domain) {
        return this.doHTTP("GET", url_str, null, null, data, content_type, auth_domain, true, false, true, true, api_token);
    }


    // execute POST over https
    /**
     *
     * @param url_str
     * @param username
     * @param password
     * @param data
     * @param content_type
     * @param auth_domain
     * @return
     */
    public String httpPost(String url_str, String username, String password, String data, String content_type, String auth_domain) {
        return this.doHTTP("POST", url_str, username, password, data, content_type, auth_domain, true, true, false, false, null);
    }
    
    /**
     *
     * @param url_str
     * @param api_token
     * @param data
     * @param content_type
     * @param auth_domain
     * @return
     */
    public String httpPostApiTokenAuth(String url_str, String api_token, String data, String content_type, String auth_domain) {
        return this.doHTTP("POST", url_str, null, null, data, content_type, auth_domain, true, true, false, true, api_token);
    }
    
    // execute POST over https
    /**
     *
     * @param url_str
     * @param username
     * @param password
     * @param data
     * @param content_type
     * @param auth_domain
     * @return
     */
    public String httpsPost(String url_str, String username, String password, String data, String content_type, String auth_domain) {
        return this.doHTTP("POST", url_str, username, password, data, content_type, auth_domain, true, true, true, false, null);
    }
    
    /**
     *
     * @param url_str
     * @param api_token
     * @param data
     * @param content_type
     * @param auth_domain
     * @return
     */
    public String httpsPostApiTokenAuth(String url_str, String api_token, String data, String content_type, String auth_domain) {
        return this.doHTTP("POST", url_str, null, null, data, content_type, auth_domain, true, true, true, true, api_token);
    }

    // execute PUT over http
    /**
     *
     * @param url_str
     * @param username
     * @param password
     * @param data
     * @param content_type
     * @param auth_domain
     * @return
     */
    public String httpPut(String url_str, String username, String password, String data, String content_type, String auth_domain) {
        return this.doHTTP("PUT", url_str, username, password, data, content_type, auth_domain, true, true, false, false, null);
    }
    
    /**
     *
     * @param url_str
     * @param api_token
     * @param data
     * @param content_type
     * @param auth_domain
     * @return
     */
    public String httpPutApiTokenAuth(String url_str, String api_token, String data, String content_type, String auth_domain) {
        return this.doHTTP("PUT", url_str, null, null, data, content_type, auth_domain, true, true, false, true, api_token);
    }
    
    // execute PUT over https
    /**
     *
     * @param url_str
     * @param username
     * @param password
     * @param data
     * @param content_type
     * @param auth_domain
     * @return
     */
    public String httpsPut(String url_str, String username, String password, String data, String content_type, String auth_domain) {
        return this.doHTTP("PUT", url_str, username, password, data, content_type, auth_domain, true, true, true, false, null);
    }
    
    /**
     *
     * @param url_str
     * @param api_token
     * @param data
     * @param content_type
     * @param auth_domain
     * @return
     */
    public String httpsPutApiTokenAuth(String url_str, String api_token, String data, String content_type, String auth_domain) {
        return this.doHTTP("PUT", url_str, null, null, data, content_type, auth_domain, true, true, true, true, api_token);
    }

    // execute PUT over http
    /**
     *
     * @param url_str
     * @param username
     * @param password
     * @param data
     * @param content_type
     * @param auth_domain
     * @param expect_response
     * @return
     */
    public String httpPut(String url_str, String username, String password, String data, String content_type, String auth_domain, boolean expect_response) {
        return this.doHTTP("PUT", url_str, username, password, data, content_type, auth_domain, expect_response, true, false, false, null);
    }
    
    /**
     *
     * @param url_str
     * @param api_token
     * @param data
     * @param content_type
     * @param auth_domain
     * @param expect_response
     * @return
     */
    public String httpPutApiTokenAuth(String url_str, String api_token, String data, String content_type, String auth_domain, boolean expect_response) {
        return this.doHTTP("PUT", url_str, null, null, data, content_type, auth_domain, expect_response, true, false, true, api_token);
    }
    
    // execute PUT over https
    /**
     *
     * @param url_str
     * @param username
     * @param password
     * @param data
     * @param content_type
     * @param auth_domain
     * @param expect_response
     * @return
     */
    public String httpsPut(String url_str, String username, String password, String data, String content_type, String auth_domain, boolean expect_response) {
        return this.doHTTP("PUT", url_str, username, password, data, content_type, auth_domain, expect_response, true, true, false, null);
    }

    /**
     *
     * @param url_str
     * @param api_token
     * @param data
     * @param content_type
     * @param auth_domain
     * @param expect_response
     * @return
     */
    public String httpsPutApiTokenAuth(String url_str, String api_token, String data, String content_type, String auth_domain, boolean expect_response) {
        return this.doHTTP("PUT", url_str, null, null, data, content_type, auth_domain, expect_response, true, true, true, api_token);
    }
    
    // execute DELETE over http
    /**
     *
     * @param url_str
     * @param username
     * @param password
     * @param data
     * @param content_type
     * @param auth_domain
     * @return
     */
    public String httpDelete(String url_str, String username, String password, String data, String content_type, String auth_domain) {
        return this.doHTTP("DELETE", url_str, username, password, data, content_type, auth_domain, true, true, false, false, null);
    }
    
    /**
     *
     * @param url_str
     * @param api_token
     * @param data
     * @param content_type
     * @param auth_domain
     * @return
     */
    public String httpDeleteApiTokenAuth(String url_str, String api_token, String data, String content_type, String auth_domain) {
        return this.doHTTP("DELETE", url_str, null, null, data, content_type, auth_domain, true, true, false, true, api_token);
    }
    
    // execute DELETE over https
    /**
     *
     * @param url_str
     * @param username
     * @param password
     * @param data
     * @param content_type
     * @param auth_domain
     * @return
     */
    public String httpsDelete(String url_str, String username, String password, String data, String content_type, String auth_domain) {
        return this.doHTTP("DELETE", url_str, username, password, data, content_type, auth_domain, true, true, true, false, null);
    }
    
    /**
     *
     * @param url_str
     * @param api_token
     * @param password
     * @param data
     * @param content_type
     * @param auth_domain
     * @return
     */
    public String httpsDeleteApiTokenAuth(String url_str, String api_token,String data, String content_type, String auth_domain) {
        return this.doHTTP("DELETE", url_str, null, null, data, content_type, auth_domain, true, true, true, true, api_token);
    }

    // get the requested path component of a given URL
    /**
     *
     * @param url
     * @param index
     * @param whole_path
     * @return
     */
    public String getPathComponent(String url, int index, boolean whole_path) {
        try {
            return this.getPathComponent(new URL(url), index, whole_path);
        }
        catch (MalformedURLException ex) {
            this.errorLogger().critical("Caught Exception parsing URL: " + url + " in getPathComponent: ", ex);
        }
        return null;
    }

    private String getPathComponent(URL url, int index, boolean whole_path) {
        String value = null;
        if (whole_path) {
            value = url.getPath().trim();
        }
        else {
            String path = url.getPath().replace("/", " ").trim();
            String list[] = path.split(" ");
            if (index >= 0 && index < list.length) {
                value = list[index];
            }
            if (index < 0) {
                return list[list.length - 1];
            }
        }
        return value;
    }
    
    private void saveResponseCode(int response_code) {
        this.m_last_response_code = response_code;
    }
    
    public int getLastResponseCode() { return this.m_last_response_code; }

    // perform an authenticated HTML operation
    @SuppressWarnings("empty-statement")
    private String doHTTP(String verb, String url_str, String username, String password, String data, String content_type, String auth_domain, boolean doInput, boolean doOutput, boolean doSSL,boolean use_api_token,String api_token) {
        String result = "";
        String line = "";
        URLConnection connection = null;
        SSLContext sc = null;

        try {
            URL url = new URL(url_str);

            // Http Connection and verb
            if (doSSL) {
                // Create a trust manager that does not validate certificate chains
                TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager(){
                    @Override
                    public X509Certificate[] getAcceptedIssuers(){return null;}
                    @Override
                    public void checkClientTrusted(X509Certificate[] certs, String authType){}
                    @Override
                    public void checkServerTrusted(X509Certificate[] certs, String authType){}
                }};

                // Install the all-trusting trust manager
                try {
                    sc = SSLContext.getInstance("TLS");
                    sc.init(null, trustAllCerts, new SecureRandom());
                    HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
                    HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
                            @Override
                            public boolean verify(String hostname, SSLSession session) {
                                return true;
                            }
                        });
                } catch (NoSuchAlgorithmException | KeyManagementException e) {
                    // do nothing
                    ;
                }
                
                // open the SSL connction
                connection = (HttpsURLConnection)(url.openConnection());
                ((HttpsURLConnection)connection).setRequestMethod(verb);
                ((HttpsURLConnection)connection).setSSLSocketFactory(sc.getSocketFactory());
                ((HttpsURLConnection)connection).setHostnameVerifier(new HostnameVerifier() {
                            @Override
                            public boolean verify(String hostname, SSLSession session) {
                                return true;
                            }
                        });
            }
            else {
                connection = (HttpURLConnection)(url.openConnection()); 
                ((HttpURLConnection)connection).setRequestMethod(verb);
            }
            
            connection.setDoInput(doInput);
            if (doOutput && data != null && data.length() > 0) {
                connection.setDoOutput(doOutput);
            }
            else {
                connection.setDoOutput(false);
            }

            // enable basic auth if requested
            if (use_api_token == false && username != null && username.length() > 0 && password != null && password.length() > 0) {
                String encoding = Base64.encodeBase64String((username + ":" + password).getBytes());
                connection.setRequestProperty("Authorization", "Basic " + encoding);
                //this.errorLogger().info("Basic Authorization: " + username + ":" + password + ": " + encoding);
            }
            
            // enable ApiTokenAuth auth if requested
            if (use_api_token == true && api_token != null && api_token.length() > 0) {
                connection.setRequestProperty("Authorization", "bearer " + api_token);
                //this.errorLogger().info("ApiTokenAuth Authorization: " + api_token);
            }

            // specify content type if requested
            if (content_type != null && content_type.length() > 0) {
                connection.setRequestProperty("Content-Type", content_type);
                connection.setRequestProperty("Accept", "*/*");
            }
            
            // add Connection: keep-alive
            connection.setRequestProperty("Connection", "keep-alive");

            // special gorp for HTTP DELETE
            if (verb != null && verb.equalsIgnoreCase("delete")) {
                connection.setRequestProperty("Access-Control-Allow-Methods", "OPTIONS, DELETE");
            }

            // specify domain if requested
            if (auth_domain != null && auth_domain.length() > 0) {
                connection.setRequestProperty("Domain", auth_domain);
            }

            // DEBUG dump the headers
            //if (doSSL) 
            //    this.errorLogger().info("HTTP: Headers: " + ((HttpsURLConnection)connection).getRequestProperties()); 
            //else
            //    this.errorLogger().info("HTTP: Headers: " + ((HttpURLConnection)connection).getRequestProperties()); 
            
            // specify data if requested - assumes it properly escaped if necessary
            if (doOutput && data != null && data.length() > 0) {
                try (OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream())) {
                    out.write(data);
                }
            }

            // setup the output if requested
            if (doInput) {
                try {
                    try (InputStream content = (InputStream) connection.getInputStream(); BufferedReader in = new BufferedReader(new InputStreamReader(content))) {
                        while ((line = in.readLine()) != null) {
                            result += line;
                        }
                    }
                }
                catch (java.io.FileNotFoundException ex) {
                    this.errorLogger().info("HTTP(" + verb + ") empty response (OK).");
                    result = "";
                }
            }
            else {
                // no result expected
                result = "";
            }
            
            // save off the HTTP response code...
            if (doSSL)
                this.saveResponseCode(((HttpsURLConnection)connection).getResponseCode());
            else 
                this.saveResponseCode(((HttpURLConnection)connection).getResponseCode());
            
            // DEBUG
            //if (doSSL)
            //    this.errorLogger().info("HTTP(" + verb +") URL: " + url_str + " Data: " + data + " Response code: " + ((HttpsURLConnection)connection).getResponseCode());
            //else
            //    this.errorLogger().info("HTTP(" + verb +") URL: " + url_str + " Data: " + data + " Response code: " + ((HttpURLConnection)connection).getResponseCode());
        }
        catch (IOException ex) {
            this.errorLogger().critical("Caught Exception in doHTTP(" + verb + "): " + ex.getMessage());
            result = null;
        }

        // return the result
        return result;
    }
}
