/**
 * @file    IoTEventHubDeviceManager.java
 * @brief   MS IoTEventHub Device Manager for the MS IoTEventHub Peer Processor
 * @author  Doug Anson
 * @version 1.0
 * @see
 *
 * Copyright 2015. ARM Ltd. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package com.arm.connector.bridge.coordinator.processors.ms;

import com.arm.connector.bridge.core.BaseClass;
import com.arm.connector.bridge.core.ErrorLogger;
import com.arm.connector.bridge.core.Utils;
import com.arm.connector.bridge.preferences.PreferenceManager;
import com.arm.connector.bridge.transport.HttpTransport;
import java.util.HashMap;
import java.util.Map;

/**
 * This class defines the required REST functions to manage IoTEventHub devices via the gateway mapping
 * @author Doug Anson
 */
public class IoTEventHubDeviceManager extends BaseClass {
    private HttpTransport           m_http = null;
    private String                  m_suffix = null;
    private HashMap<String,String>  m_endpoint_keys = null;
    private String                  m_device_id_url_template = null;
    private String                  m_api_version = null;
    private String                  m_iot_event_hub_name = null;
    private String                  m_iot_event_hub_add_device_json = null;
    private String                  m_iot_event_hub_sas_token = null;
    
     // constructor
    public IoTEventHubDeviceManager(ErrorLogger logger,PreferenceManager preferences,HttpTransport http) {
        this(logger,preferences,null,http);
    }
    
    // constructor
    public IoTEventHubDeviceManager(ErrorLogger logger,PreferenceManager preferences,String suffix,HttpTransport http) {
        super(logger,preferences);
        
        // HTTP and suffix support
        this.m_http = http;
        this.m_suffix = suffix;  
        
        // initialize the endpoint keys map
        this.m_endpoint_keys = new HashMap<>();
        
        // IoTEventHub Name
        this.m_iot_event_hub_name = this.preferences().valueOf("iot_event_hub_name",this.m_suffix);
        
        // IoTEventHub REST API Version
        this.m_api_version = this.preferences().valueOf("iot_event_hub_api_version",this.m_suffix);
        
        // IoTEventHub DeviceID REST URL Template
        this.m_device_id_url_template = this.preferences().valueOf("iot_event_hub_device_id_url",this.m_suffix).replace("__IOT_EVENT_HUB__",this.m_iot_event_hub_name).replace("__API_VERSION__", this.m_api_version);
    
        // Add device JSON template
        this.m_iot_event_hub_add_device_json = this.preferences().valueOf("iot_event_hub_add_device_json",this.m_suffix);
        
        // IoTEventHub SAS Token
        this.m_iot_event_hub_sas_token = this.preferences().valueOf("iot_event_hub_sas_token",this.m_suffix);
    }
    
    // get the endpoint access key
    public String getEndpointAccessKey(String ep_name) {
        return this.m_endpoint_keys.get(ep_name);
    }
    
    // create the endpoint device key
    private String createIoTEventHubDeviceSymkey(String ep_name,String ep_type) {
        return Utils.createDeviceKey(ep_name);
    }
    
    // process new device registration
    public Boolean registerNewDevice(Map message) {
        // create the new device type
        String device_type = (String)message.get("ept");
        String device = (String)message.get("ep");
        
        // create the URL
        String url = this.m_device_id_url_template.replace("__EPNAME__", device);
        
        // create the endpoint key
        String key = this.createIoTEventHubDeviceSymkey(device,device_type);
         
        // build out the POST payload
        String payload = this.m_iot_event_hub_add_device_json.replace("__EPNAME__", device).replace("__EPNAME_KEY__",key);
        
        // DEBUG
        this.errorLogger().info("registerNewDevice: URL: " + url + " DATA: " + payload);
        
        // dispatch and look for the result
        String result = this.put(url,payload);
        
        // DEBUG
        this.errorLogger().info("registerNewDevice: RESULT: " + result);
        
        // return our status
        Boolean status = (result != null && result.length() > 0);
        
        // save off the endpoint key
        this.m_endpoint_keys.put(device,key);
                
        // return our status
        return status;
    } 
    
    // process device de-registration
    public Boolean deregisterDevice(String device) {    
        // create the URL
        String url = this.m_device_id_url_template.replace("__EPNAME__", device);
        
        // DEBUG
        this.errorLogger().info("deregisterDevice: URL: " + url);
        
        // dispatch and look for the result
        String result = this.delete(url);
        
        // DEBUG
        this.errorLogger().info("deregisterDevice: RESULT: " + result);
        
        // return our status
        Boolean status = (result != null && result.length() > 0);
        
        // remove the endpoint key
        this.m_endpoint_keys.remove(device);
        
        // return our status
        return status;
    }
    
    // PUT specific data to a given URL (with data)
    private String put(String url,String payload) {
        this.m_http.enableUnqualifiedAuthorization(true);
        String result = this.m_http.httpPutApiTokenAuth(url,this.m_iot_event_hub_sas_token,payload,"application/json",null);
        this.m_http.enableUnqualifiedAuthorization(false);
        return result;
    }
    
    // DELETE specific data to a given URL (with data)
    private String delete(String url) { return this.delete(url,null); }
    private String delete(String url,String payload) {
        this.m_http.enableUnqualifiedAuthorization(true);
        String result = this.m_http.httpDeleteApiTokenAuth(url,this.m_iot_event_hub_sas_token,payload,"application/json",null);
        this.m_http.enableUnqualifiedAuthorization(false);
        return result;
    }
}
