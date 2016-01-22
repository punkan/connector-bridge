/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.arm.connector.bridge.coordinator;

import com.arm.connector.bridge.console.ConsoleManager;
import com.arm.connector.bridge.coordinator.processors.ibm.ibmPeerProcessorManager;
import com.arm.connector.bridge.coordinator.processors.interfaces.MDSInterface;
import com.arm.connector.bridge.coordinator.processors.arm.MDSProcessor;
import com.arm.connector.bridge.coordinator.processors.interfaces.PeerInterface;
import com.arm.connector.bridge.coordinator.processors.ms.IoTEventHubRESTProcessor;
import com.arm.connector.bridge.core.ErrorLogger;
import com.arm.connector.bridge.preferences.PreferenceManager;
import com.arm.connector.bridge.transport.HttpTransport;
import com.codesnippets4all.json.generators.JSONGenerator;
import com.codesnippets4all.json.generators.JsonGeneratorFactory;
import com.codesnippets4all.json.parsers.JSONParser;
import java.util.ArrayList;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This the primary orchestrator for the connector bridge
 * @author Doug Anson
 */
public class Orchestrator {
    private HttpServlet              m_servlet = null;
    
    private ErrorLogger              m_error_logger = null;
    private PreferenceManager        m_preference_manager = null;
    
    // mDS processor
    private MDSInterface             m_mds_rest_processor = null;
    
    // Peer processor list
    private ArrayList<PeerInterface> m_peer_processor_list = null;
    
    private ConsoleManager           m_console_manager = null;

    private HttpTransport            m_http = null;
    
    private JsonGeneratorFactory     m_json_factory = null;
    private JSONGenerator            m_json_generator = null;
    private JSONParser               m_json_parser = null;
    private boolean                  m_listeners_initialized = false;
    
    private final String             m_mds_non_domain = null;
    private String                   m_mds_domain = null;
    
    public Orchestrator(ErrorLogger error_logger,PreferenceManager preference_manager, String domain) {
        // save the error handler
        this.m_error_logger = error_logger;
        this.m_preference_manager = preference_manager;
                      
        // MDS domain is required 
        if (domain != null && domain.equalsIgnoreCase(this.preferences().valueOf("mds_def_domain")) == false)
            this.m_mds_domain = domain;
          
        // create the JSON Generator
        this.m_json_factory = JsonGeneratorFactory.getInstance();
        this.m_json_generator = this.m_json_factory.newJsonGenerator();

        // create the JSON Parser
        this.m_json_parser = new JSONParser();
        
        // build out the HTTP transport
        this.m_http = new HttpTransport(this.m_error_logger,this.m_preference_manager);
                        
        // REQUIRED: We always create the mDS REST processor
        this.m_mds_rest_processor = new MDSProcessor(this,this.m_http);
        
        // initialize our peer processor list
        this.initPeerProcessorList();
        
        // Simply link the mDS processor to the peer. The peer, when created, will automatically get linked to the mDS processor 
        this.m_mds_rest_processor.setPeerProcessorList(this.m_peer_processor_list);
        
        // create the console manager
        this.m_console_manager = new ConsoleManager(this);
    }
    
    // initialize our peer processor
    private void initPeerProcessorList() {
        // initialize the list
        this.m_peer_processor_list = new ArrayList<>();
        
        // add peer processors
        if (this.ibmPeerEnabled()) {
            // IBM/MQTT: create the MQTT processor manager
            this.errorLogger().info("Orchestrator: adding IBM IoTF/StarterKit/MQTT Processor");
            this.m_peer_processor_list.add(ibmPeerProcessorManager.createPeerProcessor(this,this.m_mds_rest_processor,this.m_http));
        }
        if (this.msPeerEnabled()) {
            // Microsoft: create IoTEventHub processor
            this.errorLogger().info("Orchestrator: adding MS IoTEventHub Processor");
            this.m_peer_processor_list.add(IoTEventHubRESTProcessor.createPeerProcessor(this,this.m_mds_rest_processor,this.m_http));
        }
    }
    
    // use IBM peer processor?
    private Boolean ibmPeerEnabled() {
        return (this.preferences().booleanValueOf("enable_iotf_addon") || this.preferences().booleanValueOf("enable_starterkit_addon"));
    }
    
    // use MS peer processor?
    private Boolean msPeerEnabled() {
        return this.preferences().booleanValueOf("enable_iot_eventhub_addon");
    }
    
    // are the listeners active?
    public boolean peerListenerActive() { 
        return this.m_listeners_initialized; 
    }
    
    // initialize peer listener
    public void initPeerListener() {
        if (!this.m_listeners_initialized) {
            // MQTT Listener
            for(int i=0;i<this.m_peer_processor_list.size();++i) {
                this.m_peer_processor_list.get(i).initListener();
            }
            this.m_listeners_initialized = true;
        }
    }
    
    // stop the peer listener
    public void stopPeerListener() {
        if (this.m_listeners_initialized) {
            // MQTT Listener
            for(int i=0;i<this.m_peer_processor_list.size();++i) {
                this.m_peer_processor_list.get(i).stopListener();
            }
            this.m_listeners_initialized = false;
        }
    }
    
    // initialize the mDS webhook
    public void initMDSWebhook() {
        if (this.m_mds_rest_processor != null) {
            this.m_mds_rest_processor.setNotificationCallbackURL();
        }
    }
    
    // reset mDS webhook
    public void resetMDSWebhook() {
        // REST (mDS)
        if (this.m_mds_rest_processor != null) {
            this.m_mds_rest_processor.resetNotificationCallbackURL();
        }
    }
    
    // process the mDS notification
    public void processNotification(HttpServletRequest request, HttpServletResponse response) {
        // process the received REST message
        //this.errorLogger().info("events (REST-" + request.getMethod() + "): " + request.getRequestURI());
        this.mds_rest_processor().processMDSMessage(request, response);
    }
    
    // process the Console request
    public void processConsole(HttpServletRequest request, HttpServletResponse response) {
        // process the received REST message
        //this.errorLogger().info("console (REST-" + request.getMethod() + "): " + request.getServletPath());
        this.console_manager().processConsole(request, response);
    }
    
    // set the HttpServlet
    private void setServlet(HttpServlet servlet) {
        this.m_servlet = servlet;
    }

    // get the HttpServlet
    public HttpServlet getServlet() {
        return this.m_servlet;
    }
    
    // get the ErrorLogger
    public ErrorLogger errorLogger() {
        return this.m_error_logger;
    }
    
    // get he Preferences manager
    public final PreferenceManager preferences() { 
        return this.m_preference_manager; 
    }
    
    // get the Peer processor
    public ArrayList<PeerInterface> peer_processor_list() {
        return this.m_peer_processor_list;
    }
    
    // get the mDS processor
    public MDSInterface mds_rest_processor() {
        return this.m_mds_rest_processor;
    }
    
    // get the console manager
    public ConsoleManager console_manager() {
        return this.m_console_manager;
    }
    
    // get our mDS domain
    public String getDomain() {
        return this.m_mds_domain;
    }
    
    // get the JSON parser instance
    public JSONParser getJSONParser() {
        return this.m_json_parser;
    }
    
    // get the JSON generation instance
    public JSONGenerator getJSONGenerator() {
        return this.m_json_generator;
    }
}
