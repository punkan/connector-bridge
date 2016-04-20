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
import com.arm.connector.bridge.preferences.PreferenceManager;
import com.arm.connector.bridge.transport.HttpTransport;
import java.util.HashMap;
import java.util.Map;

/**
 * This class defines the required REST functions to manage IoTEventHub devices via the gateway mapping
 * @author Doug Anson
 */
public class IoTEventHubDeviceManager extends BaseClass {
    private HttpTransport m_http = null;
 
    private String m_iot_event_hub_gw_client_id = null;
    
    private String m_iot_event_hub_gw_key = null;
    private String m_gw_iot_event_hub_auth_token = null;
       
    private String m_suffix = null;
    
    private HashMap<String,String> m_device_types = null;
    
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
        
        // create the device type map
        this.m_device_types = new HashMap<>();        
    }
        
    // update the IoTEventHub Username bindings to use
    public String updateUsernameBinding(String def) {
        return def;
    }
    
    // update the IoTEventHub Password bindings to use
    public String updatePasswordBinding(String def) {
        return def;
    }
    
    // update the IoTEventHub MQTT ClientID bindings to use
    public String updateClientIDBinding(String def) {
        return def;
    }  
    
    // process new device registration
    public Boolean registerNewDevice(Map message) {
        // create the new device type
        String device_type = (String)message.get("ept");
        String device = (String)message.get("ep");
        
        // create the URL
        String url = null;
        
        // add the device ID to the end
        url += "/devices";
        
        // build out the POST payload
        String payload = null; // this.createAddDeviceJSON(message);
        
        // DEBUG
        // this.errorLogger().info("registerNewDevice: URL: " + url + " DATA: " + payload + " USER: " + this.m_watson_iot_api_key + " PW: " + this.m_watson_iot_auth_token);
        
        // dispatch and look for the result
        String result = null; // this.gwpost(url, payload);
        
        // DEBUG
        this.errorLogger().info("registerNewDevice: RESULT: " + result);
        
        // return our status
        Boolean status = (result != null && result.length() > 0);
        
        // save off our device type if successful
        this.m_device_types.put(device, device_type);
        
        // return our status
        return status;
    } 
    
    // process device de-registration
    public Boolean deregisterDevice(String device) {
        String device_type = this.m_device_types.get(device);
        
        // create the URL
        String url = null; // this.createDevicesURL(device_type);
        
        // add the device ID to the end
        url += "/devices/" + device;
        
        // DEBUG
        // this.errorLogger().info("deregisterDevice: URL: " + url + " USER: " + this.m_watson_iot_api_key + " PW: " + this.m_watson_iot_auth_token);
        
        // dispatch and look for the result
        String result = null; // this.gwdelete(url);
        
        // DEBUG
        this.errorLogger().info("deregisterDevice: RESULT: " + result);
        
        // return our status
        Boolean status = (result != null && result.length() > 0);
        
        // delete our device type
        if (status == true) this.m_device_types.remove(device);
        
        // return our status
        return status;
    }
}
