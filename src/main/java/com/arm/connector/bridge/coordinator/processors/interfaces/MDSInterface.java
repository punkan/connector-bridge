/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.arm.connector.bridge.coordinator.processors.interfaces;

import java.util.ArrayList;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This interface defines the exposed methods of the mDS processor that can be used by a given peer processor.
 * @author Doug Anson
 */
public interface MDSInterface {
    // peer connection
    public void setPeerProcessorList(ArrayList<PeerInterface> manager);

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
}