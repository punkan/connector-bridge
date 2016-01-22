/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.arm.connector.bridge.coordinator.processors.interfaces;

import java.util.Map;

/**
 * This interface defines the exposed methods of a peer processor (such as a GenericMQTTProcessor) that are consumed by the mDS processor
 * @author Doug Anson
 */
public interface PeerInterface {
    // create peer-centric authentication hash for mDS webhook authentication
    public String createAuthenticationHash();
        
    // process a new endpoint registration message from mDS
    public void processNewRegistration(Map message);
    
    // process an endpoint re-registration message from mDS
    public void processReRegistration(Map message);
    
    // process an endpoint de-registration message from mDS
    public String[] processDeregistrations(Map message);
    
    // process an endpoint registrations-expired message from mDS
    public void processRegistrationsExpired(Map message);
    
    // process an endpoint async response result from mDS
    public void processAsyncResponses(Map message);
    
    // process an endpoint resource notification message from mDS
    public void processNotification(Map message);
        
    // start/stop peer listeners
    public void initListener();
    public void stopListener();
}
