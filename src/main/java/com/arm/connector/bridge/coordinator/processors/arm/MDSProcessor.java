/**
 * @file    MDSProcessor.java
 * @brief   mDS Peer Processor for the connector bridge
 * @author  Doug Anson
 * @version 1.0
 * @see
 *
 * Copyright (c) 2016 ARM
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.arm.connector.bridge.coordinator.processors.arm;

import com.arm.connector.bridge.coordinator.processors.interfaces.MDSInterface;
import com.arm.connector.bridge.coordinator.Orchestrator;
import com.arm.connector.bridge.servlet.Manager;
import com.arm.connector.bridge.coordinator.processors.core.Processor;
import com.arm.connector.bridge.core.Utils;
import com.arm.connector.bridge.transport.HttpTransport;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * mDS/mDC Peer processor for the connector bridge
 * @author Doug Anson
 */
public class MDSProcessor extends Processor implements MDSInterface {
    private HttpTransport              m_http = null;
    private String                     m_mds_host = null;
    private int                        m_mds_port = 0;
    private String                     m_mds_username = null;
    private String                     m_mds_password = null;
    private String                     m_content_type = null;
    private String                     m_api_token = null;
    private boolean                    m_use_api_token = false;
    private String                     m_mds_gw_callback = null;
    private String                     m_default_mds_uri = null;
    private String                     m_default_gw_uri = null;
    private boolean                    m_use_https_dispatch = false;
    private String                     m_mds_version = null;
    private boolean                    m_mds_gw_use_ssl = false;
    private boolean                    m_mds_use_ssl = false;
    private boolean                    m_using_callback_webhooks = false;
    private boolean                    m_disable_sync = false;
    private boolean                    m_skip_validation = false;
    
    // constructor
    public MDSProcessor(Orchestrator orchestrator,HttpTransport http) {
        super(orchestrator,null);
        this.m_http = http;
        this.m_mds_domain = orchestrator.getDomain();
        this.m_mds_host = orchestrator.preferences().valueOf("mds_address");
        this.m_mds_port = orchestrator.preferences().intValueOf("mds_port");
        this.m_mds_username = orchestrator.preferences().valueOf("mds_username");
        this.m_mds_password = orchestrator.preferences().valueOf("mds_password");
        this.m_content_type = orchestrator.preferences().valueOf("mds_content_type");
        this.m_mds_gw_callback = orchestrator.preferences().valueOf("mds_gw_callback");
        this.m_use_https_dispatch = this.prefBoolValue("mds_use_https_dispatch");
        this.m_mds_version = this.prefValue("mds_version");
        this.m_mds_gw_use_ssl = this.prefBoolValue("mds_gw_use_ssl");
        this.m_use_api_token = this.prefBoolValue("mds_use_api_token");
        if (this.m_use_api_token == true) this.m_api_token = this.orchestrator().preferences().valueOf("mds_api_token");
        
        // validation check override
        this.m_skip_validation = orchestrator.preferences().booleanValueOf("mds_skip_validation_override");
        if (this.m_skip_validation == true) {
            orchestrator.errorLogger().info("MDSProcessor: Validation Skip Override ENABLED");
        }
        
        // initialize the default type of URI for contacting us (GW) - this will be sent to mDS for the webhook URL
        this.setupBridgeURI();
        
        // initialize the default type of URI for contacting mDS
        this.setupMDSURI();
        
        // configure the callback type based on the version of mDS
        this.setupCallbackType();
        
        // sanity check the configured mDS AUTH type
        this.sanityCheckAuthType();  
        
        // disable sync usage if with Connector
        if (this.mdsIsConnector() == true) {
            this.errorLogger().info("Using mbed Device Connector. Sync=true DISABLED");
            this.m_disable_sync = true;
        }
    }
    
    // mDS requires use of SSL (mDC)
    private Boolean mdsRequiresSSL() { return this.m_mds_use_ssl; }
    
    // mDS using callback webhook vs. push-url
    private boolean mdsUsingCallbacks() { return (this.m_mds_gw_callback.equalsIgnoreCase("callback")); }
    
    // setup the bridge URI
    private void setupBridgeURI() {
        this.m_default_gw_uri = "http://";
        if (this.m_mds_gw_use_ssl) {
            this.m_default_gw_uri = "https://";
        }
    }
    
    // setup the mDS default URI
    @SuppressWarnings("empty-statement")
    private void setupMDSURI() {
        this.m_default_mds_uri = "http://";
        try {
            Double ver = Double.valueOf(this.m_mds_version);
            if (ver >= Manager.MDS_NON_DOMAIN_VER_BASE && this.m_use_api_token && this.m_use_https_dispatch == true) {
                // we are using mDS Connector... 
                this.m_default_mds_uri = "https://";
                this.m_mds_port = 443;
                
                // we assume mDS is Connector and thus requires use of SSL throughout.
                this.m_mds_use_ssl = true;
            }
        }
        catch (NumberFormatException ex) {
            // silent
            ;
        }
    }
    
    // set the callback type we are using
    @SuppressWarnings("empty-statement")
    private void setupCallbackType() {
        try {
            Double ver = Double.valueOf(this.m_mds_version);
            if (ver >= Manager.MDS_NON_DOMAIN_VER_BASE) {
                if(this.m_mds_gw_callback.equalsIgnoreCase("push-url") == true) {
                    this.m_mds_gw_callback = "callback";     // force use of callback... push-url no longer used
                }
            }
        }
        catch (NumberFormatException ex) {
            // silent
            ;
        }
        
        // set the boolean checker...
        this.m_using_callback_webhooks = (this.m_mds_gw_callback.equalsIgnoreCase("callback") == true);
    }
    
    // our the mDS notifications coming in over the webhook validatable?
    private Boolean validatableNotifications() {
        return this.m_using_callback_webhooks;
    }
    
    // sanity check the authentication type
    private void sanityCheckAuthType() {
         // sanity check...
        if (this.m_use_api_token == true && (this.m_api_token == null || this.m_api_token.length() == 0)) {
            this.orchestrator().errorLogger().warning("WARNING: API TOKEN AUTH enabled but no token found/acquired... disabling...");
            this.m_use_api_token = false;
        }
        
        // DEBUG
        if (this.useAPITokenAuth())
            this.orchestrator().errorLogger().info("Using API TOKEN Authentication");
        else
            this.orchestrator().errorLogger().info("Using BASIC Authentication");
    }
    
    // is our mDS instance actually mDC?
    private boolean mdsIsConnector() {
        return (this.m_use_api_token == true && this.m_using_callback_webhooks == true && this.m_use_https_dispatch == true);
    }
    
    // mDS is using Token Auth
    private boolean useAPITokenAuth() { 
        return this.m_use_api_token; 
    }
    
    // validate the notification
    private Boolean validateNotification(HttpServletRequest request) {
        boolean validated = false;
        if (this.validatableNotifications() == true && request.getHeader("Authentication") != null) {
            String calc_hash = this.createAuthenticationHash();
            String header_hash = request.getHeader("Authentication");
            validated = Utils.validateHash(header_hash,calc_hash);
            
            // DEBUG
            if (!validated) {
                this.errorLogger().warning("validateNotification: failed: calc: " + calc_hash + " header: " + header_hash);
            }
            
            // override
            if (this.m_skip_validation == true) {
                validated = true;
            }
            
            
            // return validation status
            return validated;
        }
        else {
            // using push-url. No authentication possible.
            return true;
        }
    }
    
    // create any authentication header JSON that may be necessary
    private String createCallbackHeaderAuthJSON() {
        return "{\"Authentication\":\"" + this.createAuthenticationHash() +  "\"}";
    }
    
    // create our callback URL
    private String createCallbackURL() {
        String url = null;
        
        String local_ip = Utils.getExternalIPAddress(); // this.prefValue("mds_gw_address");
        int local_port = this.prefIntValue("mds_gw_port");
        if (this.m_mds_gw_use_ssl == true) ++local_port;        // SSL will use +1 of this port... ensure firewall configs match!
        String notify_uri = this.prefValue("mds_gw_context_path") + this.prefValue("mds_gw_events_path") + this.getDomain(true);
 
        // build and return the webhook callback URL
        return  this.m_default_gw_uri + local_ip + ":" + local_port + notify_uri;
    }
    
    // create the dispatch URL for changing the notification URL
    private String createDispatchURL() {
        return this.createBaseURL() + this.getDomain() + "/notification/" + this.m_mds_gw_callback;
    }
    
    // get the currently configured callback URL
    private String getNotificationCallbackURL() {
        String url = null;
        String headers = null;
        
        // create the dispatch URL
        String dispatch_url = this.createDispatchURL();
        
        // Issue GET and look at the response
        String json = null;
        
        // SSL vs. HTTP
        if (this.m_use_https_dispatch == true) {
            // get the callback URL (SSL)
            json = this.httpsGet(dispatch_url);
        }
        else {
            // get the callback URL
            json =this.httpGet(dispatch_url);
        }
        try {
            if (json != null && json.length() > 0) {
                if (this.m_mds_gw_callback.equalsIgnoreCase("callback")) {
                    // JSON parser does not like "headers":{}... so map it out
                    json = json.replace(",\"headers\":{}", "");
                          
                    // Callback API used: parse the JSON
                    Map parsed = (Map)this.parseJson(json.replace(",\"headers\":{}", ""));
                    url = (String)parsed.get("url");
                    
                    // headers are optional...
                    try {
                        headers = (String)parsed.get("headers");
                    }
                    catch (Exception json_ex) {
                        headers = "";
                    }

                    // DEBUG
                    this.orchestrator().errorLogger().info("getNotificationCallbackURL(callback): url: " + url + " headers: " + headers + " dispatch: " + dispatch_url);
                }
                else {
                    // use the Deprecated push-url API... (no JSON)
                    url = json;

                    // DEBUG
                    this.orchestrator().errorLogger().info("getNotificationCallbackURL(push-url): url: " + url + " dispatch: " + dispatch_url);
                }
            }
            else {
                // no response received back from mDS
                this.orchestrator().errorLogger().warning("getNotificationCallbackURL: no response recieved from dispatch: " + dispatch_url);
            }
        }
        catch (Exception ex) {
            this.orchestrator().errorLogger().warning("getNotificationCallbackURL: exception: " + ex.getMessage() + ". json=" + json);
        }
        
        return url;
    }
    
    
    // determine if our callback URL has already been set
    private boolean notificationCallbackURLSet(String target_url) {
        return this.notificationCallbackURLSet(target_url,false);
    }
    
    // determine if our callback URL has already been set
    private boolean notificationCallbackURLSet(String target_url,boolean skip_check) {
        String current_url = this.getNotificationCallbackURL();
        this.errorLogger().info("notificationCallbackURLSet: current_url: " + current_url + " target_url: " + target_url);
        boolean is_set = (target_url != null && current_url != null && target_url.equalsIgnoreCase(current_url)); 
        if (is_set == true && this.mdsUsingCallbacks() && skip_check == false) {
            // for Connector, lets ensure that we always have the expected Auth Header setup. So, while the same, lets delete and re-install...
            this.errorLogger().info("notificationCallbackURLSet(callback): deleting existing callback URL...");
            this.removeNotificationCallback();
            this.errorLogger().info("notificationCallbackURLSet(callback): re-establishing callback URL...");
            this.setNotificationCallbackURL(target_url,skip_check); // skip_check, go ahead and assume we need to set it...
            this.errorLogger().info("notificationCallbackURLSet(callback): re-checking that callback URL is properly set...");
            current_url = this.getNotificationCallbackURL();
            is_set = (target_url != null && current_url != null && target_url.equalsIgnoreCase(current_url));
        }
        return is_set;
    }
    
    // remove the mDS Connector Notification Callback
    private void removeNotificationCallback() {
        // create the dispatch URL
        String dispatch_url = this.createDispatchURL();
        
        // SSL vs. HTTP
        if (this.m_use_https_dispatch == true) {
            // delete the callback URL (SSL)
            this.httpsDelete(dispatch_url);
        }
        else {
            // delete the callback URL
            this.httpDelete(dispatch_url);
        }
    }
    
    // reset the mDS Notification Callback URL
    @Override
    public void resetNotificationCallbackURL() {
        if (this.validatableNotifications() == true) {
            // we simply delete the webhook 
            this.removeNotificationCallback(); 
        }
        else {       
            // we reset to default
            String default_url = this.prefValue("mds_default_notify_url");
            this.errorLogger().info("resetNotificationCallbackURL: resetting notification URL to: " + default_url);
            this.setNotificationCallbackURL(default_url);
        }
    }
    
    // set our mDS Notification Callback URL
    @Override
    public void setNotificationCallbackURL() {
        String target_url = this.createCallbackURL();
        this.setNotificationCallbackURL(target_url);
    }
    
    
    // set our mDS Notification Callback URL
    private void setNotificationCallbackURL(String target_url) {
        this.setNotificationCallbackURL(target_url,true); // default is to check if the URL is already set... 
    }
    
    // set our mDS Notification Callback URL
    private void setNotificationCallbackURL(String target_url,boolean check_url_set) {
        boolean callback_url_already_set = false; // assume default is that the URL is NOT set... 
        
        // we must check to see if we want to check that the URL is already set...
        if (check_url_set == true) {
            // go see if the URL is already set.. 
            callback_url_already_set = this.notificationCallbackURLSet(target_url);
        }
        
        // proceed to set the URL if its not already set.. 
        if (!callback_url_already_set) {    
            String dispatch_url = this.createDispatchURL();
            String auth_header_json = this.createCallbackHeaderAuthJSON();
            String json = null;
            
            // build out the callback JSON
            if (this.m_mds_gw_callback.equalsIgnoreCase("callback")) {
                // use the Callback API
                if (auth_header_json == null)
                    json =  "{ \"url\" :\"" + target_url + "\" }";
                else
                    json =  "{ \"url\" :\"" + target_url + "\", \"headers\":" + auth_header_json + "}";

                // DEBUG
                this.errorLogger().info("setNotificationCallbackURL(callback): json: " + json + " dispatch: " + dispatch_url);
            }
            else {
                // use the Deprecated push-url API... (no JSON)
                json = target_url;
                
                // DEBUG
                this.errorLogger().info("setNotificationCallbackURL(push-url): url: " + json + " dispatch: " + dispatch_url);
            }
            
            // SSL vs. HTTP
            if (this.m_use_https_dispatch == true) {
                // set the callback URL (SSL)
                this.httpsPut(dispatch_url,json);
            }
            else {
                // set the callback URL
                this.httpPut(dispatch_url,json);
            }
            
            // check that it succeeded
            if (!this.notificationCallbackURLSet(target_url,!check_url_set)) {
                this.errorLogger().warning("setNotificationCallbackURL: ERROR: unable to set callback URL to: " + target_url);
            }
            else {
                // DEBUG
                this.errorLogger().info("setNotificationCallbackURL: notification URL set to: " + target_url + " (SUCCESS)");
            }
        }
        else {
            // DEBUG
            this.errorLogger().info("setNotificationCallbackURL: notification URL already set to: " + target_url + " (OK)");
        }
    }
    
    // unregister endpoint resource
    private void unregisterNotificationResource(String endpoint,Map resource) {
        // create the subscription URL...
        String uri = "";
        if (resource != null) uri = (String)resource.get("path");
        String url = this.createEndpointResourceSubscriptionURL(uri);
        this.errorLogger().info("unregisterNotificationResource: sending endpoint resource subscription removal request: " + url);
        this.httpDelete(url);                      
    }
    
    // de-register endpoints
    @Override
    public void processDeregistrations(String[] endpoints) {
        for(int i=0;i<endpoints.length;++i) {
            // create the endpoint subscription URL...
            String url = this.createBaseURL() + this.getDomain() + "/endpoints/" + endpoints[i];
            this.errorLogger().info("unregisterEndpoint: sending endpoint subscription removal request: " + url);
            this.httpDelete(url);   
        }
    }
    
    // create the Endpoint Subscription Notification URL
    private String createEndpointResourceSubscriptionURL(String uri) {
        return this.createEndpointResourceSubscriptionURL(uri,null);
    } 
    
    // create the Endpoint Subscription Notification URL
    private String createEndpointResourceSubscriptionURL(String uri,Map options) {
        // build out the URL for mDS Endpoint notification subscriptions...
        String url = this.createBaseURL() + uri;
        
        // SYNC Usage
        
        // add options if present
        if (options != null  && this.m_disable_sync == false) {
            // valid options...
            String sync = (String)options.get("sync");
            
            // construct the query string...
            String qs = "";
            qs = this.buildQueryString(qs,"sync",sync);
            if (qs != null && qs.length() > 0) url = url + "?" + qs;
        }
        
        // DEBUG
        this.errorLogger().info("createEndpointResourceSubscriptionURL: " + url);
        
        // return the endpoint notification subscription URL
        return url;
    }
    
    // create the Endpoint Subscription Notification URL (default options)
    private String createEndpointResourceSubscriptionURL(String domain,String endpoint,String uri) {
        HashMap<String,String> options = new HashMap<>();
        
        // SYNC Usage
        if (this.m_disable_sync == false) {
            options.put("sync","true");
        }
        return this.createEndpointResourceSubscriptionURL(domain, endpoint, uri, options);
    }
    
    // create the Endpoint Subscription Notification URL
    private String createEndpointResourceSubscriptionURL(String domain,String endpoint,String uri,Map<String,String> options) {
        // build out the URL for mDS Endpoint notification subscriptions...
        // /{domain}/subscriptions/{endpoint-name}/{resource-path}?sync={true&#124;false}
        String url = this.createBaseURL() + domain  + "/subscriptions/" + endpoint + uri;
        
        // SYNC Usage 
        
        // add options if present
        if (options != null && this.m_disable_sync == false) {
            // valid options...
            String sync = (String)options.get("sync");
            
            // construct the query string...
            String qs = "";
            qs = this.buildQueryString(qs,"sync",sync);
            if (qs != null && qs.length() > 0) url = url + "?" + qs;
        }
        
        // DEBUG
        this.errorLogger().info("createEndpointResourceSubscriptionURL: " + url);
        
        // return the endpoint notification subscription URL
        return url;
    }
    
    // create the Endpoint Resource Request URL 
    private String createEndpointResourceRequestURL(String uri,Map options) {
        // build out the URL for mDS Endpoint Resource requests...
        String url = this.createBaseURL() + uri;
        
        // add options if present
        if (options != null) {
            // valid options...
            String sync = (String)options.get("sync");
            String cacheOnly = (String)options.get("cacheOnly");
            String noResp = (String)options.get("noResp");
            String pri = (String)options.get("pri");
            
            // construct the query string...
            String qs = "";
            
            // SYNC Usage
            if (this.m_disable_sync == false ) {
                qs = this.buildQueryString(qs,"sync",sync);
            }
            
            qs = this.buildQueryString(qs,"cacheOnly",cacheOnly);
            qs = this.buildQueryString(qs,"noResp",noResp);
            qs = this.buildQueryString(qs,"pri",pri);
            if (qs != null && qs.length() > 0) url = url + "?" + qs;
        }
        
        // DEBUG
        this.errorLogger().info("createEndpointResourceRequestURL: " + url);
        
        // return the endpoint resource request URL
        return url;
    }
    
    // create the Endpoint Resource Discovery URL 
    private String createEndpointResourceDiscoveryURL(String uri) {
        // build out the URL for mDS Endpoint Resource discovery...
        String url = this.createBaseURL() + uri;
        
        // no options available...
     
        // DEBUG
        //this.errorLogger().info("createEndpointResourceDiscoveryURL: " + url);
        
        // return the endpoint resource discovery URL
        return url;
    }
    
    // create the Endpoint Discovery URL 
    private String createEndpointDiscoveryURL(Map options) {
        // build out the URL for mDS Endpoint Discovery...
        String url = this.createBaseURL() + this.getDomain() + "/endpoints";
        
        // add options if present
        if (options != null) {
            // valid options...
            String type = (String)options.get("type");
            String stale = (String)options.get("stale");
            
            // construct the query string...
            String qs = "";
            qs = this.buildQueryString(qs,"type",type);
            qs = this.buildQueryString(qs,"stale",stale);
            if (qs != null && qs.length() > 0) url = url + "?" + qs;
        }
        
        // DEBUG
        //this.errorLogger().info("createEndpointDiscoveryURL: " + url);
        
        // return the discovery URL
        return url;
    }
    
    // invoke HTTP GET request
    private String httpsGet(String url) {
        String response = null;
        if (this.useAPITokenAuth()) {
            response = this.m_http.httpsGetApiTokenAuth(url, this.m_api_token, null, this.m_content_type, this.m_mds_domain);
        }
        else {
            response = this.m_http.httpsGet(url, this.m_mds_username, this.m_mds_password, null, this.m_content_type, this.m_mds_domain);
        }
        this.errorLogger().info("httpsGet: response: " + this.m_http.getLastResponseCode());
        return response;
    }
    
    // invoke HTTP GET request
    private String httpGet(String url) {
        String response = null;
        if (this.useAPITokenAuth()) {
            response = this.m_http.httpGetApiTokenAuth(url, this.m_api_token, null, this.m_content_type, this.m_mds_domain);
        }
        else {
            response = this.m_http.httpGet(url, this.m_mds_username, this.m_mds_password, null, this.m_content_type, this.m_mds_domain);
        }
        this.errorLogger().info("httpGet: response: " + this.m_http.getLastResponseCode());
        return response;
    }
    
    // invoke HTTP PUT request (SSL)
    private String httpsPut(String url) {
        return this.httpsPut(url,null);
    }
    
    // invoke HTTP PUT request (SSL)
    private String httpsPut(String url,String data) {
        String response = null;
        if (this.useAPITokenAuth()) {
            response = this.m_http.httpsPutApiTokenAuth(url, this.m_api_token, data, this.m_content_type, this.m_mds_domain);
        }
        else {
            response = this.m_http.httpsPut(url, this.m_mds_username, this.m_mds_password, data, this.m_content_type, this.m_mds_domain);
        }
        this.errorLogger().info("httpsPut: response: " + this.m_http.getLastResponseCode());
        return response;
    }
    
    // invoke HTTP PUT request
    private String httpPut(String url) {
        return this.httpPut(url,null);
    }
    
    // invoke HTTP PUT request
    private String httpPut(String url,String data) {
        String response = null;
        if (this.useAPITokenAuth()) {
            response = this.m_http.httpPutApiTokenAuth(url, this.m_api_token, data, this.m_content_type, this.m_mds_domain);
        }
        else {
            response = this.m_http.httpPut(url, this.m_mds_username, this.m_mds_password, data, this.m_content_type, this.m_mds_domain);
        }
        this.errorLogger().info("httpPut: response: " + this.m_http.getLastResponseCode());
        return response;
    }
    
    // invoke HTTP POST request (SSL)
    private String httpsPost(String url) {
        return this.httpsPost(url,null);
    }
    
    // invoke HTTP POST request (SSL)
    private String httpsPost(String url,String data) {
        String response = null;
        if (this.useAPITokenAuth()) {
            response = this.m_http.httpsPostApiTokenAuth(url, this.m_api_token, data, this.m_content_type, this.m_mds_domain);
        }
        else {
            response = this.m_http.httpsPost(url, this.m_mds_username, this.m_mds_password, data, this.m_content_type, this.m_mds_domain);
        }
        this.errorLogger().info("httpsPost: response: " + this.m_http.getLastResponseCode());
        return response;
    }
    
    // invoke HTTP POST request
    private String httpPost(String url) {
        return this.httpPost(url,null);
    }
    
    // invoke HTTP POST request
    private String httpPost(String url,String data) {
        String response = null;
        if (this.useAPITokenAuth()) {
            response = this.m_http.httpPostApiTokenAuth(url, this.m_api_token, data, this.m_content_type, this.m_mds_domain);
        }
        else {
            response = this.m_http.httpPost(url, this.m_mds_username, this.m_mds_password, data, this.m_content_type, this.m_mds_domain);
        }
        this.errorLogger().info("httpPost: response: " + this.m_http.getLastResponseCode());
        return response;
    }
    
    // invoke HTTP DELETE request
    private String httpsDelete(String url) {
        String response = null;
        if (this.useAPITokenAuth()) {
            response = this.m_http.httpsDeleteApiTokenAuth(url, this.m_api_token, null, this.m_content_type, this.m_mds_domain);
        }
        else {
            response = this.m_http.httpsDelete(url, this.m_mds_username, this.m_mds_password, null, this.m_content_type, this.m_mds_domain);
        }
        this.errorLogger().info("httpDelete: response: " + this.m_http.getLastResponseCode());
        return response;
    }
    
    // invoke HTTP DELETE request
    private String httpDelete(String url) {
        String response = null;
        if (this.useAPITokenAuth()) {
            response = this.m_http.httpDeleteApiTokenAuth(url, this.m_api_token, null, this.m_content_type, this.m_mds_domain);
        }
        else {
            response = this.m_http.httpDelete(url, this.m_mds_username, this.m_mds_password, null, this.m_content_type, this.m_mds_domain);
        }
        this.errorLogger().info("httpDelete: response: " + this.m_http.getLastResponseCode());
        return response;
    }
    
    // process the notification
    @Override
    public void processMDSMessage(HttpServletRequest request, HttpServletResponse response) {
        // build the response
        String response_header = "";
        String json = this.read(request);
        
        // process and route the mDS message
        this.processMDSMessage(json, request);
        
        // send the response back as an ACK to mDS
        this.sendResponseToMDS("text/html;charset=UTF-8", request, response, "", "");
    }
    
    // process and route the mDS message to the appropriate peer method
    private void processMDSMessage(String json,HttpServletRequest request) {
        // DEBUG
        //this.orchestrator().errorLogger().info("processMDSMessage(mDS): Received message from mDS: " + json);
        
        // tell the orchestrator to call its peer processors with this mDS message
        try {
            if (json != null && json.length() > 0 && json.equalsIgnoreCase("{}") == false) {
                Map parsed = (Map)this.parseJson(json);
                if (parsed != null) {
                    if (parsed.containsKey("notifications")) {
                        if (this.validateNotification(request)) {
                            // DEBUG
                            //this.errorLogger().info("processMDSMessage: notification VALIDATED");

                            // validated notification... process it...
                            this.orchestrator().processNotification(parsed);
                        }
                        else {
                            // validation FAILED. Note but do not process...
                            this.errorLogger().warning("processMDSMessage(mDS): notification validation FAILED. Not processed (OK)");
                        }
                    }

                    // DEBUG
                    //this.errorLogger().info("processMDSMessage(STD) Parsed: " + parsed);

                    // act on the request...
                    if (parsed.containsKey("registrations")) this.orchestrator().processNewRegistration(parsed);
                    if (parsed.containsKey("reg-updates")) this.orchestrator().processReRegistration(parsed);
                    if (parsed.containsKey("de-registrations")) this.orchestrator().processDeregistrations(parsed);
                    if (parsed.containsKey("registrations-expired")) this.orchestrator().processRegistrationsExpired(parsed);
                    if (parsed.containsKey("async-responses")) this.orchestrator().processAsyncResponses(parsed);
                }
                else {
                    // parseJson() failed...
                    this.errorLogger().warning("processMDSMessage(mDS): unable to parse JSON: " + json);
                }
            }
            else {
                // empty JSON... so not parsed
                this.errorLogger().warning("processMDSMessage(mDS): empty JSON not parsed (OK).");
            }
        }
        catch (Exception ex) {
            // exception during JSON parsing
            this.errorLogger().warning("processMDSMessage(mDS) Exception during notification body JSON parsing: " + json, ex);
        }
    }
    
    // process an endpoint resource subscription request
    @Override
    public String subscribeToEndpointResource(String uri,Map options,Boolean init_webhook) {
        String url = this.createEndpointResourceSubscriptionURL(uri,options);
        return this.subscribeToEndpointResource(url,init_webhook);
    }
    
    // process an endpoint resource subscription request
    @Override
    public String subscribeToEndpointResource(String ep_name,String uri,Boolean init_webhook) {
        String url = this.createEndpointResourceSubscriptionURL(this.getDomain(),ep_name,uri);
        return this.subscribeToEndpointResource(url,init_webhook);
    }
    
    // subscribe to endpoint resources
    private String subscribeToEndpointResource(String url,Boolean init_webhook) {
        if (init_webhook) {
            this.errorLogger().info("subscribeToEndpointResource: (re)setting the event notification URL...");
            this.setNotificationCallbackURL();
        }
        
        String json = null;
        this.errorLogger().info("subscribeToEndpointResource: (re)establishing subscription request: " + url);
        if (this.mdsRequiresSSL()) {
            json = this.httpsPut(url);
        }
        else { 
            json = this.httpPut(url); 
        }
        return json;
    }
    
    // process endpoint resource operation request
    @Override
    public String processEndpointResourceOperation(String verb,String ep_name,String uri) {
        return this.processEndpointResourceOperation(verb, ep_name, uri, null);
    }
    
    // process endpoint resource operation request
    @Override
    public String processEndpointResourceOperation(String verb,String ep_name,String uri,String value) {
        String json = null;
        String url = this.createCoAPURL(ep_name, uri);
        
        // dispatch the mDS REST based on CoAP verb received
        if (verb.equalsIgnoreCase(("get"))) {
            this.errorLogger().info("processEndpointResourceOperation: Invoking GET: " + url);
            json = this.httpGet(url);
        }
        if (verb.equalsIgnoreCase(("put"))) {
            this.errorLogger().info("processEndpointResourceOperation: Invoking PUT: " + url + " DATA: " + value);
            json = this.httpPut(url,value);
        }
        if (verb.equalsIgnoreCase(("post"))) {
            this.errorLogger().info("processEndpointResourceOperation: Invoking POST: " + url + " DATA: " + value);
            json = this.httpPost(url,value);
        }
        if (verb.equalsIgnoreCase(("del"))) {
            this.errorLogger().info("processEndpointResourceOperation: Invoking DELETE: " + url);
            json = this.httpDelete(url);
        }
        
        return json;
    }
    
    // process endpoint resource operation request
    @Override
    public String processEndpointResourceOperation(String verb,String uri,Map options) {
        String json = null;
        String url = this.createEndpointResourceRequestURL(uri,options);
        
        // DEBUG
        this.errorLogger().info("processEndpointResourceOperation: Invoking " + verb + ": " + url);
                
        // Get Endpoint Resource Value - use HTTP GET
        if (verb.equalsIgnoreCase("get")) {
            if (this.mdsRequiresSSL()) {
                json = this.httpsGet(url);
            }
            else { 
                json = this.httpGet(url); 
            }
        }
        
        // Put Endpoint Resource Value - use HTTP PUT
        if (verb.equalsIgnoreCase("put")) {
            String new_value = (String)options.get("new_value");
            if (this.mdsRequiresSSL()) {
                json = this.httpsPut(url,new_value);
            }
            else { 
                json = this.httpPut(url,new_value); 
            }
        }
        
        return json;
    }
    
    // process an endpoint resource un-subscribe request
    @Override
    public String unsubscribeFromEndpointResource(String uri,Map options) {
        String url = this.createEndpointResourceSubscriptionURL(uri,options);
        String json = null;
        this.errorLogger().info("unsubscribeFromEndpointResource: sending subscription delete request: " + url);
        if (this.mdsRequiresSSL()) {
            json = this.httpsDelete(url);
        }
        else { 
            json = this.httpDelete(url);   
        }
        return json;
    }
    
    // perform device discovery
    @Override
    public String performDeviceDiscovery(Map options) {
        String url = this.createEndpointDiscoveryURL(options);
        String json = null;

        // mDS expects request to come as a http GET
        if (this.mdsRequiresSSL()) {
            json = this.httpsGet(url);
        }
        else { 
            json = this.httpGet(url);
        }
        return json;
    }
    
    // perform device resource discovery
    @Override
    public String performDeviceResourceDiscovery(String uri) {
        String url = this.createEndpointResourceDiscoveryURL(uri);
        String json = null;
        if (this.mdsRequiresSSL()) {
            json = this.httpsGet(url);
        }
        else { 
            json = this.httpGet(url);   
        }
        return json;
    }
    
    // read the request data
    @SuppressWarnings("empty-statement")
    private String read(HttpServletRequest request) {
        String data = "";
        
        try {
            BufferedReader reader = request.getReader();
            String line = reader.readLine();
            while(line != null) {
                data += line;
                line = reader.readLine();
            }
        }
        catch (IOException ex) {
            // do nothing
            ;
        }
        
        return data;
    }
    
    // send the REST response back to mDS
    private void sendResponseToMDS(String content_type, HttpServletRequest request, HttpServletResponse response, String header, String body) {
        try {            
            response.setContentType(content_type);
            response.setHeader("Pragma", "no-cache");
            try (PrintWriter out = response.getWriter()) {
                if (header != null && header.length() > 0) out.println(header);
                if (body != null && body.length() > 0) out.println(body);
            }
        }
        catch (Exception ex) {
            this.errorLogger().critical("Unable to send REST response", ex);
        }
    }
    
    // create the base URL for mDS operations
    private String createBaseURL() {
        return this.m_default_mds_uri + this.m_mds_host + ":" + this.m_mds_port;
    }
    
    // create the CoAP operation URL
    private String createCoAPURL(String ep_name,String uri) {
        String sync_option = "";
        
        // SYNC Usage
        if (this.m_disable_sync == false) {
            sync_option = "?sync=true";
        }
        
        String url = this.createBaseURL() + this.getDomain() + "/endpoints/" + ep_name + uri + sync_option;
        return url;
    }
    
    // build out the query string
    private String buildQueryString(String qs,String key,String value) {
        String updated_qs = qs;
        
        if (updated_qs != null && key != null && value != null) {
            if (updated_qs.length() == 0) {
                updated_qs = key  + "=" + value;
            }
            else {
                if (updated_qs.contains(key) == false) {
                    updated_qs = updated_qs + "&" + key + "=" + value;
                }
                else {
                    // attempted overwrite of previously set value
                    this.errorLogger().warning("attempted overwrite of option: " + key + "=" + value + " in qs: " + updated_qs);
                }
            }
        }
        
        return updated_qs;
    }
}
