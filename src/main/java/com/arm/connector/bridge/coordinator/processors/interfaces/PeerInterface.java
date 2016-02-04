/**
 * @file    PeerInterface.java
 * @brief   Generic Peer Interface for the connector bridge
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
