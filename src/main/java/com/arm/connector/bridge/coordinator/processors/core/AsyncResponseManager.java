/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.arm.connector.bridge.coordinator.processors.core;

import com.arm.connector.bridge.coordinator.Orchestrator;
import com.arm.connector.bridge.coordinator.processors.ibm.GenericMQTTProcessor;
import com.arm.connector.bridge.core.ErrorLogger;
import com.arm.connector.bridge.transport.MQTTTransport;
import com.codesnippets4all.json.parsers.JSONParser;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Doug Anson
 */
public class AsyncResponseManager {
    private HashMap<String,HashMap<String,Object>> m_responses;
    private Orchestrator                       m_manager;
    
    public AsyncResponseManager(Orchestrator manager) {
        this.m_manager = manager;
        this.m_responses = new HashMap<>();
    }
    
    // get the error logger
    private Orchestrator manager() {
        return this.m_manager;
    }
    
    // get the error logger
    private ErrorLogger errorLogger() {
        return this.manager().errorLogger();
    }
    
    // get the async response ID
    private String id(Map response) {
        return (String)response.get("id");
    }
        
    // do we have a recording for a given AsyncResponse?
    private boolean haveRecordForAsyncResponse(String id) {
        return (this.m_responses.containsKey(id) == true);
    }
    
    // record an AsyncResponse
    public void recordAsyncResponse(String response,String coap_verb,MQTTTransport mqtt,GenericMQTTProcessor proc,String response_topic, String message, String ep_name, String uri) {
        // create a new AsyncResponse record
        HashMap<String,Object> record = new HashMap<>();
       
        // fill it... 
        record.put("verb", coap_verb);
        record.put("response",response);
        record.put("mqtt",mqtt);
        record.put("proc",proc);
        record.put("response_topic",response_topic);
        record.put("message",message);
        record.put("ep_name",ep_name);
        record.put("uri",uri);
        
        // parse the
        JSONParser parser = this.manager().getJSONParser();
        Map parsed = parser.parseJson(response);
        
        // add it to the record too
        record.put("response_map",parsed);
        
        // add the record to our list
        this.m_responses.put((String)parsed.get("async-response-id"), record);
        
        // DEBUG
        this.errorLogger().info("recordAsyncResponse: Adding Record: ID:" + (String)parsed.get("async-response-id") + " RECORD: " + record);
    }

    // process AsyncResponse
    @SuppressWarnings("empty-statement")
    public void processAsyncResponse(Map response) {
        // get our AsyncResponse ID
        String id = this.id(response);
        
        // do we have a record for this AsyncResponse?
        if (this.haveRecordForAsyncResponse(id) == true) {
            // Get the record
            HashMap<String,Object> record = this.m_responses.get(id);
            
            // pull the requisite elements from the record
            MQTTTransport mqtt = (MQTTTransport)record.get("mqtt");
            String response_topic = (String)record.get("response_topic");
            String verb = (String)record.get("verb");
            GenericMQTTProcessor proc = (GenericMQTTProcessor)record.get("proc");
            
            // construct the reply message value
            String reply = proc.formatAsyncResponseAsReply(response,verb);
            if (reply != null) {
                // DEBUG
                this.errorLogger().info("processAsyncResponse: sending reply(" + verb + ") to AsyncResponse: ID: " + id + " Topic: " + response_topic + " Message: " + reply);

                // send the reply...
                mqtt.sendMessage(response_topic, reply);
            }
            else {
                // DEBUG
                this.errorLogger().info("processAsyncResponse: not sending reply(" + verb + ") to AsyncResponse: ID: " + id + " (OK).");
            }
            
            // DEBUG
            this.errorLogger().info("processAsyncResponse: Removing record for AsyncResponse: ID: " + id);
                    
            // finally delete the record
            this.m_responses.remove(id);
        }
        else {
            // processing something we have no record on...
            ;
            
            // DEBUG
            //this.errorLogger().info("processAsyncResponse: No AsyncResponse record for ID: " + id + " Ignoring: " + response.toString());
        }
    }
}
