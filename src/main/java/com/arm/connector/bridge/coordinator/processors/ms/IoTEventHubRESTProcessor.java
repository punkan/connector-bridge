/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.arm.connector.bridge.coordinator.processors.ms;

import com.arm.connector.bridge.coordinator.Orchestrator;
import com.arm.connector.bridge.coordinator.processors.core.Processor;
import com.arm.connector.bridge.coordinator.processors.interfaces.MDSInterface;
import com.arm.connector.bridge.coordinator.processors.interfaces.PeerInterface;
import com.arm.connector.bridge.core.Utils;
import com.arm.connector.bridge.transport.HttpTransport;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;

/**
 * Microsoft IoTEventHub peer processor
 * @author Doug Anson
 */
public class IoTEventHubRESTProcessor extends Processor implements PeerInterface {
    private HttpTransport  m_http = null;
    private MDSInterface   m_mds_processor = null;
    private String         m_suffix = null;
   
    // Factory method for initializing the MS IoTEeventHub peer
    public static IoTEventHubRESTProcessor createPeerProcessor(Orchestrator manager,MDSInterface mds_rest_processor,HttpTransport http) {
        // create me
        IoTEventHubRESTProcessor me = new IoTEventHubRESTProcessor(manager,http);
        
        // initialize me
        
        // return me
        return me;
    }
    
    // constructor
    public IoTEventHubRESTProcessor(Orchestrator manager,HttpTransport http) {
        this(manager,http,null);
    }
    
    // constructor
    public IoTEventHubRESTProcessor(Orchestrator manager,HttpTransport http,String suffix) {
        super(manager,null);
        this.m_http = http;
        this.m_mds_domain = manager.getDomain();
        this.m_mds_processor = manager.mds_rest_processor();         // mDC REST processor
        this.m_suffix = suffix;
    }
    
    // process a received new registration
    @Override
    public void processNewRegistration(Map message) {
        // XXX to do
    }
    
    // process a received new registration
    @Override
    public void processReRegistration(Map message) {
        // XXX to do
    }
    
    // process a received new registration
    @Override
    public String[] processDeregistrations(Map message) {
        // XXX to do
        return null;
    }
    
    // process mds registrations-expired messages 
    @Override
    public void processRegistrationsExpired(Map message) {
        this.processDeregistrations(message);
    }
    
    // process a received new registration
    @Override
    public void processAsyncResponses(Map data) {
        // XXX to do
    }
    
    // process a received new registration/registration update/deregistration, 
    protected void processRegistration(Map data,String key) {
        // XXXX TO DO 
    }
    
    // process an observation
    @Override
    public void processNotification(Map data) {
        // XXXX TO DO
    }
    
    // Create the authentication hash
    @Override
    public String createAuthenticationHash() {
        // XXX TO DO
        return "";
    }
  
    // initialize any IotEventHub listeners
    @Override
    public void initListener() {
        // XXX to do
    }

    // stop our IoTEventHub listeners
    @Override
    public void stopListener() {
        // XXX to do
    }
}