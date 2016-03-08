/**
 * @file    MDSInterface.java
 * @brief   mDS Peer Interface for the connector bridge
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

package com.arm.connector.bridge.coordinator.processors.interfaces;

import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This interface defines the exposed methods of the mDS processor that can be used by a given peer processor.
 * @author Doug Anson
 */
public interface MDSInterface {
    // process notifications
    public void processMDSMessage(HttpServletRequest request, HttpServletResponse response);
    
    // process de-registrations
    public void processDeregistrations(String[] deregistrations);
    
    // process resource subscription request
    public String subscribeToEndpointResource(String uri,Map options,Boolean init_webhook);
    public String subscribeToEndpointResource(String ep_name,String uri,Boolean init_webhook);
    
    // process resource un-subscribe request
    public String unsubscribeFromEndpointResource(String uri,Map options);
    
    // perform device discovery
    public String performDeviceDiscovery(Map options);
    
    // perform device resource discovery
    public String performDeviceResourceDiscovery(String uri);
    
    // perform CoAP operations on endpoint resources
    public String processEndpointResourceOperation(String verb,String uri,Map options);
    public String processEndpointResourceOperation(String verb,String ep_name,String uri);
    public String processEndpointResourceOperation(String verb,String ep_name,String uri,String value);
       
    // Webhook management
    public void setNotificationCallbackURL();
    public void resetNotificationCallbackURL();
    
    // Device Metadata extraction
    public void pullDeviceMetadata(Map endpoint);
    
    // Begin mDS/mDS webhook and subscription validation polling...
    public void beginValidationPolling();
}