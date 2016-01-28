/**
 * @file    IoTFDeviceManager.java
 * @brief   IBM IoTF Device Manager for the IBM IoTF Peer Processor
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
package com.arm.connector.bridge.coordinator.processors.ibm;

import com.arm.connector.bridge.core.BaseClass;
import com.arm.connector.bridge.core.ErrorLogger;
import com.arm.connector.bridge.core.Utils;
import com.arm.connector.bridge.preferences.PreferenceManager;
import com.arm.connector.bridge.transport.HttpTransport;
import java.util.List;
import java.util.Map;

/**
 * This class defines the required REST functions to manage IoTF devices via the gateway mapping
 * @author Doug Anson
 */
public class IoTFDeviceManager extends BaseClass {
    private HttpTransport m_http = null;
 
    private String m_iotf_rest_uri_template = null;
    private String m_iotf_add_device_template = null;
    private String m_iotf_add_gw_type_template = null;
    private String m_iotf_add_gw_dev_type_template = null;
    private String m_iotf_add_gw_template = null;
    private String m_iotf_rest_hostname = null;
    
    private String m_iotf_gw_id = null;
    private String m_iotf_gw_type_id = null;
    private String m_iotf_gw_dev_type_id = null;
    private String m_iotf_gw_auth_token = null;
    
    private String m_iotf_org_id = null;
    private String m_iotf_api_key = null;
    private String m_iotf_password = null;
    
    private String m_iotf_username = null;
    private String m_iotf_gw_client_id = null;
    
    private String m_gw_iotf_username = null;
    private String m_gw_iotf_password = null;
       
    private String m_suffix = null;
    
     // constructor
    public IoTFDeviceManager(ErrorLogger logger,PreferenceManager preferences,HttpTransport http) {
        this(logger,preferences,null,http);
    }
    
    // constructor
    public IoTFDeviceManager(ErrorLogger logger,PreferenceManager preferences,String suffix,HttpTransport http) {
        super(logger,preferences);
        
        // HTTP and suffix support
        this.m_http = http;
        this.m_suffix = suffix;
        
        // pull the needed configuration/preferences
        this.m_iotf_gw_id = this.preferences().valueOf("iotf_gw_id",this.m_suffix) + Utils.getExternalIPAddress().replace(".","");
        this.m_iotf_gw_type_id = this.preferences().valueOf("iotf_gw_type_id",this.m_suffix);
        this.m_iotf_gw_dev_type_id = this.preferences().valueOf("iotf_gw_dev_type_id",this.m_suffix);
        
        this.m_iotf_org_id = this.preferences().valueOf("iotf_org_id",this.m_suffix);
        this.m_iotf_api_key = this.preferences().valueOf("iotf_api_key",this.m_suffix);
       
        this.m_iotf_rest_uri_template = this.preferences().valueOf("iotf_rest_uri_template",this.m_suffix);
        this.m_iotf_add_gw_type_template =  this.preferences().valueOf("iotf_add_gw_type_template",this.m_suffix);
        this.m_iotf_add_gw_dev_type_template =  this.preferences().valueOf("iotf_add_gw_dev_type_template",this.m_suffix);
        this.m_iotf_add_gw_template =  this.preferences().valueOf("iotf_add_gw_template",this.m_suffix);
        this.m_iotf_add_device_template = this.preferences().valueOf("iotf_add_device_template",this.m_suffix);
        this.m_iotf_rest_hostname = this.preferences().valueOf("iotf_rest_hostname_template",this.m_suffix).replace("__ORG_ID__",this.m_iotf_org_id);
        
        // build out Basic Auth username/password for the REST calls
        this.m_iotf_username = "a-" + this.m_iotf_org_id + "-" + this.m_iotf_api_key;
        this.m_gw_iotf_username = "g-" + this.m_iotf_org_id + "-" + this.m_iotf_gw_type_id + "-" + this.m_iotf_gw_id;
        this.m_iotf_password = this.preferences().valueOf("iotf_password",this.m_suffix);
        this.m_iotf_gw_auth_token = Utils.createURLSafeToken(this.m_iotf_password);
    }
    
    // update the OrgID and APIKey
    public void updateIoTFBindings(String org_id,String api_key) {
        this.m_iotf_org_id = org_id;
        this.m_iotf_api_key = api_key;
        this.m_iotf_rest_hostname = this.preferences().valueOf("iotf_rest_hostname_template",this.m_suffix).replace("__ORG_ID__",this.m_iotf_org_id);
        this.m_iotf_username = "a-" + this.m_iotf_org_id + "-" + this.m_iotf_api_key;
        this.m_gw_iotf_username = "g-" + this.m_iotf_org_id + "-" + this.m_iotf_gw_type_id + "-" + this.m_iotf_gw_id;
        this.m_gw_iotf_password = this.m_iotf_gw_auth_token;
        this.initIoTFMetadata();
    }
    
    // initialize the IoTF Gateway/Device/Type metadata
    private void initIoTFMetadata() {
        // ensure we are initialized properly
        this.createGatewayType();
        this.createGatewayDeviceType();
        this.createGatewayDevice();
                
        // initialize the Gateway client ID
        this.m_iotf_gw_client_id = "g:" + this.m_iotf_org_id + ":" + this.m_iotf_gw_type_id + ":" + this.m_iotf_gw_id;
    }
    
    // update the IoTF Username bindings to use
    public String updateUsernameBinding(String def) {
        // using Token Auth
        return "use-token-auth";
    }
    
    // update the IoTF Password bindings to use
    public String updatePasswordBinding(String def) {
        return this.m_iotf_gw_auth_token;
    }
    
    // update the IoTF MQTT ClientID bindings to use
    public String updateClientIDBinding(String def) {
        return this.m_iotf_gw_client_id;
    }
    
    // update the IoTF MQTT Hostname bindings to use
    public String updateHostnameBinding(String def) {
        return this.preferences().valueOf("iotf_rest_hostname_template",this.m_suffix).replace("__ORG_ID__",this.m_iotf_org_id + ".messaging");
    }
    
    // update the IoTF MQTT Hostname bindings to use
    public String updateDeviceType(String def) {
        return this.m_iotf_gw_dev_type_id; 
    }
    
    // check and build out the gateway type
    private void createGatewayType() {
        if (!this.hasGatewayType()) {
            this.installGatewayType();
        }
    }
    
    // check and build out the gateway device type
    private void createGatewayDeviceType() {
        if (!this.hasGatewayDeviceType()) {
            this.installGatewayDeviceType();
        }
    }
    
    // check and build out the gateway device
    private void createGatewayDevice() {
        if (!this.hasGatewayDevice()) {
            this.installGatewayDevice();
        }
    }
    
    // ensure we have a gateway type
    private Boolean hasGatewayType() {
        String result = this.get(this.createGatewayURL());
        return (result != null && result.contains(this.m_iotf_gw_type_id) == true);
    }
    // ensure we have a gateway device type
    private Boolean hasGatewayDeviceType() {
        String result = this.get(this.createDevicesURL());
        return (result != null && result.contains(this.m_iotf_gw_dev_type_id) == true);
    }
    
    // ensure we have a gateway device
    private Boolean hasGatewayDevice() {
        String result = this.get(this.createGatewayURL() + "/devices");
        return (result != null && result.contains(this.m_iotf_gw_id) == true);
    }
 
    // install the Gateway Type 
    private Boolean installGatewayType() {
        // create the URL
        String url = "https://" + this.m_iotf_rest_hostname + this.m_iotf_rest_uri_template;
        
        // build out the POST payload
        String payload = this.createAddGatewayTypeJSON();
        
        // DEBUG
        //this.errorLogger().info("installGatewayType: URL: " + url + " DATA: " + payload + " USER: " + this.m_iotf_username + " PW: " + this.m_iotf_password);
        
        // dispatch and look for the result
        String result = this.post(url, payload);
        
        // DEBUG
        this.errorLogger().info("installGatewayType: RESULT: " + result);
        
        // return our status
        return (result != null && result.length() > 0);
    }
    
    // install the Gateway Device Type 
    private Boolean installGatewayDeviceType() {
        // create the URL
        String url = "https://" + this.m_iotf_rest_hostname + this.m_iotf_rest_uri_template;
        
        // build out the POST payload
        String payload = this.createAddGatewayDeviceTypeJSON();
        
        // DEBUG
        //this.errorLogger().info("installGatewayDeviceType: URL: " + url + " DATA: " + payload + " USER: " + this.m_iotf_username + " PW: " + this.m_iotf_password);
        
        // dispatch and look for the result
        String result = this.post(url, payload);
        
        // DEBUG
        this.errorLogger().info("installGatewayDeviceType: RESULT: " + result);
        
        // return our status
        return (result != null && result.length() > 0);
    }
    
    // install the Gateway Device 
    private Boolean installGatewayDevice() {
        // create the URL
        String url = this.createGatewayURL();
        
        // add the device ID to the end
        url += "/devices";
        
        // build out the POST payload
        String payload = this.createAddGatewayJSON();
        
        // DEBUG
        //this.errorLogger().info("installGatewayDeviceType: URL: " + url + " DATA: " + payload + " USER: " + this.m_iotf_username + " PW: " + this.m_iotf_password);
        
        // dispatch and look for the result
        String result = this.post(url, payload);
        
        // DEBUG
        this.errorLogger().info("installGatewayDeviceType: RESULT: " + result);
        
        // return our status
        return (result != null && result.length() > 0);
    }
    
    // build the REST URI for device management
    private String buildDevicesURI() {
        return this.m_iotf_rest_uri_template + "/" + this.m_iotf_gw_dev_type_id;
    }
    
    // build the REST URI for device gateway management
    private String buildGatewayURI() {
        return this.m_iotf_rest_uri_template + "/" + this.m_iotf_gw_type_id;
    }
    
    // build out the REST URL for device management
    private String createDevicesURL() {
        return "https://" + this.m_iotf_rest_hostname + this.buildDevicesURI(); 
    }
    
    // build out the REST URL for device gateway management
    private String createGatewayURL() {
        return "https://" + this.m_iotf_rest_hostname + this.buildGatewayURI(); 
    }
    
    // GET specific URL contents
    private String get(String url) {
        String result = this.m_http.httpGet(url, this.m_iotf_username, this.m_iotf_password, null, "application/json", null);
        //this.errorLogger().info("get: URL: " + url + " RESULT: " + result);
        return result;
    }
    
    // POST specific data to a given URL
    private String post(String url,String payload) {
        String result = this.m_http.httpsPost(url, this.m_iotf_username, this.m_iotf_password, payload, "application/json", null);
        //this.errorLogger().info("post: URL: " + url + " DATA: " + payload + " RESULT: " + result);
        return result;
    }
    
    // POST specific data to a given URL
    private String gwpost(String url,String payload) {
        String result = this.m_http.httpsPost(url, this.m_gw_iotf_username, this.m_gw_iotf_password, payload, "application/json", null);
        //this.errorLogger().info("post: URL: " + url + " DATA: " + payload + " RESULT: " + result);
        return result;
    }
    
    // DELETE specific data to a given URL
    private String delete(String url) {
        String result = this.delete(url,null);
        this.errorLogger().info("delete: URL: " + url + " RESULT: " + result);
        return result;
    }
    
    // DELETE specific data to a given URL (with data)
    private String delete(String url,String payload) {
        return this.m_http.httpsDelete(url, this.m_iotf_username, this.m_iotf_password, payload, "application/json", null);
    }
    
    // DELETE specific data to a given URL
    private String gwdelete(String url) {
        String result = this.delete(url,null);
        this.errorLogger().info("delete: URL: " + url + " RESULT: " + result);
        return result;
    }
    
    // DELETE specific data to a given URL (with data)
    private String gwdelete(String url,String payload) {
        return this.m_http.httpsDelete(url, this.m_gw_iotf_username, this.m_gw_iotf_password, payload, "application/json", null);
    }
    
    // build out the metadata JSON
    private String createMetadataJSON(Map metadata) {
        // initialize
        String json = "{";
        
        // loop through resources
        List resources = (List)metadata.get("resources");
        for(int i=0;resources != null && i<resources.size();++i) {
            // add ith resource as metadata
            Map resource = (Map)resources.get(i);
            
            // build out
            json += "\"res" + i + "\":";
            json += "\"" + (String)resource.get("path") + "\"";
            if (i < (resources.size()-1)) {
                json += ",";
            }
        }
        
        // add the CoAP Endpoint Type too...
        if (resources != null && resources.size() > 0) json += ",";
        json += "\"ept\":";
        json += "\"" + (String)metadata.get("ept") + "\""; 
        
        // finish up
        json += "}";
        
        return json;
    }
    
    // build out the metadata JSON
    private String createMetadataDeviceInfoJSON(Map metadata) {
        // deviceInfo JSON construction
        String json = ",\"deviceInfo\": {";
        
        // SerialNumber
        json += "\"serialNumber\":\"" + (String)metadata.get("meta_serial") + "\",";
        
        // Manufacturer
        json += "\"manufacturer\":\"" + (String)metadata.get("meta_mfg") + "\",";
        
        // Model
        json += "\"model\":\"" + (String)metadata.get("meta_model") + "\",";
        
        // Device Class
        json += "\"deviceClass\":\"" + (String)metadata.get("meta_class") + "\",";
        
        // Description
        json += "\"description\":\"" + (String)metadata.get("meta_description") + "\",";
        
        // Firmware
        json += "\"fwVersion\":\"" + (String)metadata.get("meta_firmware") + "\",";
        
        // Hardware
        json += "\"hwVersion\":\"" + (String)metadata.get("meta_hardware") + "\",";
        
        // Location description
        json += "\"descriptiveLocation\":\"" + (String)metadata.get("meta_location") + "\"";
        
        // finish
        json += "}";
        
        // return the device info
        return json;
    }
    
    // build out the ADD Gateway Type JSON
    private String createAddGatewayTypeJSON() {
        return this.m_iotf_add_gw_type_template.replace("__GW_TYPE_ID__", this.m_iotf_gw_type_id);
    }
    
    // build out the ADD Gateway Device Type JSON
    private String createAddGatewayDeviceTypeJSON() {
        return this.m_iotf_add_gw_dev_type_template.replace("__TYPE_ID__",this.m_iotf_gw_dev_type_id);
    }
    
    // build out the ADD Gateway Device JSON
    private String createAddGatewayJSON() {
        return this.m_iotf_add_gw_template.replace("__GW_ID__", this.m_iotf_gw_id)
                                          .replace("__AUTH__",this.m_iotf_gw_auth_token);
    }
    
    // build out the ADD device json
    private String createAddDeviceJSON(String deviceID,Map metadata) {
        return this.m_iotf_add_device_template.replace("__DEVICE_ID__", deviceID)
                                              .replace("__GW_ID__", this.m_iotf_gw_id)
                                              .replace("__GW_TYPE_ID__", this.m_iotf_gw_type_id)
                                              .replace("__AUTH__",this.m_iotf_gw_auth_token)
                                              .replace("__METADATA__",this.createMetadataJSON(metadata))
                                              .replace("__DEVICE_INFO__",this.createMetadataDeviceInfoJSON(metadata));
    }
    
    // create the AddDevice JSON from the message map
    private String createAddDeviceJSON(Map message) {
        // DEBUG
        this.errorLogger().info("createDeviceJSON: message: " + message);
        
        // pull relevant values... fill in the rest
        return this.createAddDeviceJSON((String)message.get("ep"),message);
    }
    
    // process new device registration
    public Boolean registerNewDevice(Map message) {
        // create the URL
        String url = this.createDevicesURL();
        
        // add the device ID to the end
        url += "/devices";
        
        // build out the POST payload
        String payload = this.createAddDeviceJSON(message);
        
        // DEBUG
        //this.errorLogger().info("registerNewDevice: URL: " + url + " DATA: " + payload + " USER: " + this.m_iotf_username + " PW: " + this.m_iotf_password);
        
        // dispatch and look for the result
        String result = this.gwpost(url, payload);
        
        // DEBUG
        this.errorLogger().info("registerNewDevice: RESULT: " + result);
        
        // return our status
        return (result != null && result.length() > 0);
    }
    
    // process device de-registration
    public Boolean deregisterDevice(String device) {
        // create the URL
        String url = this.createDevicesURL();
        
        // add the device ID to the end
        url += "/devices/" + device;
        
        // DEBUG
        //this.errorLogger().info("deregisterDevice: URL: " + url + " USER: " + this.m_iotf_username + " PW: " + this.m_iotf_password);
        
        // dispatch and look for the result
        String result = this.gwdelete(url);
        
        // DEBUG
        this.errorLogger().info("deregisterDevice: RESULT: " + result);
        
        // return our status
        return (result != null && result.length() > 0);
    }
}
