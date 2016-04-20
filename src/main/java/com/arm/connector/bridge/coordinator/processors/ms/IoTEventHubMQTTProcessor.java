/**
 * @file    IoTEventHubMQTTProcessor.java
 * @brief   MS IoTEventHub MQTT Peer Processor
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

import com.arm.connector.bridge.coordinator.processors.arm.GenericMQTTProcessor;
import com.arm.connector.bridge.coordinator.Orchestrator;
import com.arm.connector.bridge.coordinator.processors.interfaces.PeerInterface;
import com.arm.connector.bridge.core.Utils;
import com.arm.connector.bridge.transport.HttpTransport;
import com.arm.connector.bridge.transport.MQTTTransport;
import com.arm.connector.bridge.core.Transport;
import com.arm.connector.bridge.json.JSONParser;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

/**
 * IBM IoTEventHub peer processor based on MQTT with MessageSight
 * @author Doug Anson
 */
public class IoTEventHubMQTTProcessor extends GenericMQTTProcessor implements Transport.ReceiveListener, PeerInterface {
    public static int                NUM_COAP_VERBS = 4;                                   // GET, PUT, POST, DELETE
    private String                   m_mqtt_ip_address = null;
    private int                      m_mqtt_port = 0;
    private String                   m_iot_event_hub_name = null;
    
    private String                   m_iot_event_hub_observe_notification_topic = null;
    private String                   m_iot_event_hub_coap_cmd_topic_get = null;
    private String                   m_iot_event_hub_coap_cmd_topic_put = null;
    private String                   m_iot_event_hub_coap_cmd_topic_post = null;
    private String                   m_iot_event_hub_coap_cmd_topic_delete = null;
    private HashMap<String,Object>   m_iot_event_hub_endpoints = null;
    private String                   m_client_id_template = null;
    private Boolean                  m_use_clean_session = false;
    
    private String                   m_iot_event_hub_username = null;
    private String                   m_iot_event_hub_password = null;
        
    // IoTEventHub Device Manager
    private IoTEventHubDeviceManager m_iot_event_hub_device_manager = null;
        
    // constructor (singleton)
    public IoTEventHubMQTTProcessor(Orchestrator manager,MQTTTransport mqtt,HttpTransport http) {
        this(manager,mqtt,null,http);
    }
    
    // constructor (with suffix for preferences)
    public IoTEventHubMQTTProcessor(Orchestrator manager,MQTTTransport mqtt,String suffix,HttpTransport http) {
        super(manager,mqtt,suffix,http);
        
        // IoTEventHub Processor Announce
        this.errorLogger().info("MS IoTEventHub Processor ENABLED.");
        
        // initialize the endpoint map
        this.m_iot_event_hub_endpoints = new HashMap<>();
                        
        // get our defaults
        this.m_iot_event_hub_name = this.orchestrator().preferences().valueOf("iot_event_hub_name",this.m_suffix);
        this.m_mqtt_ip_address = this.orchestrator().preferences().valueOf("iot_event_hub_mqtt_ip_address",this.m_suffix).replace("__IOT_EVENT_HUB__",this.m_iot_event_hub_name);
        this.m_mqtt_port = this.orchestrator().preferences().intValueOf("iot_event_hub_mqtt_port",this.m_suffix);
                
        // starter kit supports observation notifications
        this.m_iot_event_hub_observe_notification_topic = this.orchestrator().preferences().valueOf("iot_event_hub_observe_notification_topic",this.m_suffix).replace("__EVENT_TYPE__","observation"); 
        
        // starter kit can send CoAP commands back through mDS into the endpoint via these Topics... 
        this.m_iot_event_hub_coap_cmd_topic_get = this.orchestrator().preferences().valueOf("iot_event_hub_coap_cmd_topic",this.m_suffix).replace("__COMMAND_TYPE__","get");
        this.m_iot_event_hub_coap_cmd_topic_put = this.orchestrator().preferences().valueOf("iot_event_hub_coap_cmd_topic",this.m_suffix).replace("__COMMAND_TYPE__","put");
        this.m_iot_event_hub_coap_cmd_topic_post = this.orchestrator().preferences().valueOf("iot_event_hub_coap_cmd_topic",this.m_suffix).replace("__COMMAND_TYPE__","post");
        this.m_iot_event_hub_coap_cmd_topic_delete = this.orchestrator().preferences().valueOf("iot_event_hub_coap_cmd_topic",this.m_suffix).replace("__COMMAND_TYPE__","delete");
                        
        // IoTEventHub Device Manager - will initialize and update our IoTEventHub bindings/metadata
        this.m_iot_event_hub_device_manager = new IoTEventHubDeviceManager(this.orchestrator().errorLogger(),this.orchestrator().preferences(),this.m_suffix,http);
        
        // create the client ID
        this.m_client_id = null;
        
        // set the MQTT username and password
        this.m_iot_event_hub_username = null;
        this.m_iot_event_hub_password = null;
        
        // create the transport
        mqtt.setUsername(this.m_iot_event_hub_username);
        mqtt.setPassword(this.m_iot_event_hub_password);
                
        // add the transport
        this.initMQTTTransportList();
        this.addMQTTTransport(this.m_client_id, mqtt);
            
        // DEBUG
        //this.errorLogger().info("IoTEventHub Credentials: Username: " + this.m_mqtt.getUsername() + " PW: " + this.m_mqtt.getPassword());
    }
    
    // OVERRIDE: Connection to IoTEventHub vs. stock MQTT...
    @Override
    protected boolean connectMQTT() {
        return this.mqtt().connect(this.m_mqtt_ip_address,this.m_mqtt_port,this.m_client_id,this.m_use_clean_session);
    }
    
    // OVERRIDE: (Listening) Topics for IoTEventHub vs. stock MQTT...
    @Override
    @SuppressWarnings("empty-statement")
    protected void subscribeToMQTTTopics() {
        // do nothing... IoTEventHub will have "listenable" topics for the CoAP verbs via the CMD event type...
        ;
    }
    
    // OVERRIDE: process a mDS notification for IoTEventHub
    @Override
    public void processNotification(Map data) {
        // DEBUG
        //this.errorLogger().info("processNotification(IoTEventHub)...");
        
        // get the list of parsed notifications
        List notifications = (List)data.get("notifications");
        for(int i=0;notifications != null && i<notifications.size();++i) {
            // we have to process the payload... this may be dependent on being a string core type... 
            Map notification = (Map)notifications.get(i);
            
            // decode the Payload...
            String b64_coap_payload = (String)notification.get("payload");
            String decoded_coap_payload = Utils.decodeCoAPPayload(b64_coap_payload);
            
            // Try a JSON parse... if it succeeds, assume the payload is a composite JSON value...
            Map json_parsed = this.tryJSONParse(decoded_coap_payload);
            if (json_parsed != null) {
                // add in a JSON object payload value directly... 
                notification.put("value", json_parsed);
            }
            else {
                // add in a decoded payload value as a string type...
                notification.put("value", decoded_coap_payload);
            }
                        
            // we will send the raw CoAP JSON... IoTEventHub can parse that... 
            String coap_raw_json = this.jsonGenerator().generateJson(notification);
            
            // strip off []...
            String coap_json_stripped = this.stripArrayChars(coap_raw_json);
            
            // get our endpoint name
            String ep_name = (String)notification.get("ep");
            
            // encapsulate into a coap/device packet...
            // XXX
                                    
            // DEBUG
            //this.errorLogger().info("IoTEventHub: CoAP notification: " + iot_event_hub_coap_json);
            this.errorLogger().info("IoTEventHub: CoAP notification (JSON): " + notification);
            
            // send to IoTEventHub...
            // XXX this.mqtt().sendMessage(this.customizeTopic(this.m_iot_event_hub_observe_notification_topic,ep_name,this.m_iot_event_hub_device_manager.getDeviceType(ep_name)),iot_event_hub_coap_json,QoS.AT_MOST_ONCE);           
         }
    }
    
    // OVERRIDE: process a re-registration in IoTEventHub
    @Override
    public void processReRegistration(Map data) {
        List notifications = (List)data.get("reg-updates");
        for(int i=0;notifications != null && i<notifications.size();++i) {
            Map entry = (Map)notifications.get(i);
            this.errorLogger().info("IoTEventHub : CoAP re-registration: " + entry);
            boolean do_register = this.unsubscribe((String)entry.get("ep"));
            if (do_register == true) 
                this.processRegistration(data,"reg-updates");
            else 
                this.subscribe((String)entry.get("ep"),(String)entry.get("ept"));
        }
    }
    
    // OVERRIDE: handle de-registrations for IoTEventHub
    @Override
    public String[] processDeregistrations(Map parsed) {
        String[] deregistration = super.processDeregistrations(parsed);
        for(int i=0;deregistration != null && i<deregistration.length;++i) {
            // DEBUG
            this.errorLogger().info("IoTEventHub : CoAP de-registration: " + deregistration[i]);
            
            // IoTEventHub add-on... 
            this.unsubscribe(deregistration[i]);
            
            // Remove from IoTEventHub
            this.deregisterDevice(deregistration[i]);
        }
        return deregistration;
    }
    
    // OVERRIDE: process mds registrations-expired messages 
    @Override
    public void processRegistrationsExpired(Map parsed) {
        this.processDeregistrations(parsed);
    }
    
    // OVERRIDE: process a received new registration for IoTEventHub
    @Override
    public void processNewRegistration(Map data) {
        this.processRegistration(data,"registrations");
    }
    
    // OVERRIDE: process a received new registration for IoTEventHub
    @Override
    protected void processRegistration(Map data,String key) {  
        List endpoints = (List)data.get(key);
        for(int i=0;endpoints != null && i<endpoints.size();++i) {
            Map endpoint = (Map)endpoints.get(i);            
            List resources = (List)endpoint.get("resources");
            for(int j=0;resources != null && j<resources.size();++j) {
                Map resource = (Map)resources.get(j); 
                
                // re-subscribe
                if (this.m_subscriptions.containsSubscription(this.m_mds_domain,(String)endpoint.get("ep"),(String)resource.get("path"))) {
                    // re-subscribe to this resource
                    this.orchestrator().subscribeToEndpointResource((String)endpoint.get("ep"),(String)resource.get("path"),false);
                    
                    // SYNC: here we dont have to worry about Sync options - we simply dispatch the subscription to mDS and setup for it...
                    this.m_subscriptions.removeSubscription(this.m_mds_domain,(String)endpoint.get("ep"),(String)resource.get("path"));
                    this.m_subscriptions.addSubscription(this.m_mds_domain,(String)endpoint.get("ep"),(String)resource.get("path"));
                }
                
                // auto-subscribe
                else if (this.isObservableResource(resource) && this.m_auto_subscribe_to_obs_resources == true) {
                    // auto-subscribe to observable resources... if enabled.
                    this.orchestrator().subscribeToEndpointResource((String)endpoint.get("ep"),(String)resource.get("path"),false);
                    
                    // SYNC: here we dont have to worry about Sync options - we simply dispatch the subscription to mDS and setup for it...
                    this.m_subscriptions.removeSubscription(this.m_mds_domain,(String)endpoint.get("ep"),(String)resource.get("path"));
                    this.m_subscriptions.addSubscription(this.m_mds_domain,(String)endpoint.get("ep"),(String)resource.get("path"));
                }
            }    
            
            // pre-populate the new endpoint with initial values for registration
            this.orchestrator().pullDeviceMetadata(endpoint);
            
            try {
                // create the device in IoTEventHub
                this.errorLogger().info("processRegistration: calling registerNewDevice(): " + endpoint);
                this.registerNewDevice(endpoint);
                this.errorLogger().info("processRegistration: registerNewDevice() completed");
            }
            catch (Exception ex) {
                this.errorLogger().warning("processRegistration: caught exception in registerNewDevice(): " + endpoint,ex); 
            }
            
            try {
                // subscribe for IoTEventHub as well..
                this.errorLogger().info("processRegistration: calling subscribe(): " + endpoint);
                this.subscribe((String)endpoint.get("ep"),(String)endpoint.get("ept"));
                this.errorLogger().info("processRegistration: subscribe() completed");
            }
            catch (Exception ex) {
                this.errorLogger().warning("processRegistration: caught exception in registerNewDevice(): " + endpoint,ex); 
            }
        }
    }
    
    // create the IoTEventHub clientID
    private String createIoTEventHubClientID(String domain) {
        int length = 12;
        if (domain == null) domain = this.prefValue("mds_def_domain",this.m_suffix);
        if (domain.length() < 12) length = domain.length();
        return this.m_client_id_template + domain.substring(0,length);  // 12 digits only of the domain
    }
    
    // create the StarterKit compatible clientID
    private String createStarterKitClientID(String domain) {
        //
        // StarterKit clientID format:  "d:<org>:<type>:<device id>"
        // Where:
        // org - "quickstart" 
        // type - we list it as "iotsample-mbed"
        // deviceID - we bring in the custom name
        //
        String device_id = this.prefValue("iot_event_hub_starterkit_device_id"); 
        return "d:quickstart:iotsample-mbed-k64f:" + device_id;
    }
    
    // create the endpoint IoTEventHub topic data
    private HashMap<String,Object> createEndpointTopicData(String ep_name,String ep_type) {
        HashMap<String,Object> topic_data = null;
        if (this.m_iot_event_hub_coap_cmd_topic_get != null) {
            Topic[] list = new Topic[NUM_COAP_VERBS];
            String[] topic_string_list = new String[NUM_COAP_VERBS];
            topic_string_list[0] = this.customizeTopic(this.m_iot_event_hub_coap_cmd_topic_get,ep_name,ep_type);
            topic_string_list[1] = this.customizeTopic(this.m_iot_event_hub_coap_cmd_topic_put,ep_name,ep_type);
            topic_string_list[2] = this.customizeTopic(this.m_iot_event_hub_coap_cmd_topic_post,ep_name,ep_type);
            topic_string_list[3] = this.customizeTopic(this.m_iot_event_hub_coap_cmd_topic_delete,ep_name,ep_type);
            for(int i=0;i<NUM_COAP_VERBS;++i) {
                list[i] = new Topic(topic_string_list[i],QoS.AT_LEAST_ONCE);
            }
            topic_data = new HashMap<>();
            topic_data.put("topic_list",list);
            topic_data.put("topic_string_list",topic_string_list);
        }
        return topic_data;
    }
    
    private String customizeTopic(String topic,String ep_name,String ep_type) {
        String cust_topic = topic.replace("__EPNAME__", ep_name).replace("__DEVICE_TYPE__", ep_type);
        this.errorLogger().info("IoTEventHub Customized Topic: " + cust_topic); 
        return cust_topic;
    }
    
    // connect
    public boolean connect() {
        // if not connected attempt
        if (!this.isConnected()) {
            if (this.mqtt().connect(this.m_mqtt_ip_address, this.m_mqtt_port, this.m_client_id, true)) {
                this.orchestrator().errorLogger().info("IoTEventHub: Setting CoAP command listener...");
                this.mqtt().setOnReceiveListener(this);
                this.orchestrator().errorLogger().info("IoTEventHub: connection completed successfully");
            }
        }
        else {
            // already connected
            this.orchestrator().errorLogger().info("IoTEventHub: Already connected (OK)...");
        }
        
        // return our connection status
        this.orchestrator().errorLogger().info("IoTEventHub: Connection status: " + this.isConnected());
        return this.isConnected();
    }
    
    // disconnect
    public void disconnect() {
        if (this.isConnected()) {
            this.mqtt().disconnect();
        }
    }
    
    // are we connected
    private boolean isConnected() {
        if (this.mqtt() != null) return this.mqtt().isConnected();
        return false;
    }
    
    // subscribe to the IoTEventHub MQTT topics
    private void subscribe_to_topics(Topic topics[]) {
        // (4/7/16): OFF
        // this.mqtt().subscribe(topics);
        
        // (4/7/16): subscribe to each topic individually
        for(int i=0;i<topics.length;++i) {
            Topic[] single_topic = new Topic[1];
            single_topic[0] = topics[i];
            this.mqtt().subscribe(single_topic);
        }
    }
    
    // register topics for CoAP commands
    @SuppressWarnings("empty-statement")
    public void subscribe(String ep_name,String ep_type) {
        if (ep_name != null) {
            // DEBUG
            this.orchestrator().errorLogger().info("IoTEventHub: Subscribing to CoAP command topics for endpoint: " + ep_name);
            try {
                HashMap<String,Object> topic_data = this.createEndpointTopicData(ep_name,ep_type);
                if (topic_data != null) {
                    // get,put,post,delete enablement
                    this.m_iot_event_hub_endpoints.put(ep_name, topic_data);
                    this.subscribe_to_topics((Topic[])topic_data.get("topic_list"));
                }
                else {
                    this.orchestrator().errorLogger().warning("IoTEventHub: GET/PUT/POST/DELETE topic data NULL. GET/PUT/POST/DELETE disabled");
                }
            }
            catch (Exception ex) {
                this.orchestrator().errorLogger().info("IoTEventHub: Exception in subscribe for " + ep_name + " : " + ex.getMessage());
            }
        }
        else {
            this.orchestrator().errorLogger().info("IoTEventHub: NULL Endpoint name in subscribe()... ignoring...");
        }
    }
    
    // un-register topics for CoAP commands
    public boolean unsubscribe(String ep_name) {
        boolean do_register = false;
        if (ep_name != null) {
            // DEBUG
            this.orchestrator().errorLogger().info("IoTEventHub: Un-Subscribing to CoAP command topics for endpoint: " + ep_name);
            try {
                HashMap<String,Object> topic_data = (HashMap<String,Object>)this.m_iot_event_hub_endpoints.get(ep_name);
                if (topic_data != null) {
                    // unsubscribe...
                    this.mqtt().unsubscribe((String[])topic_data.get("topic_string_list"));
                } 
                else {
                    // not in subscription list (OK)
                    this.orchestrator().errorLogger().info("IoTEventHub: Endpoint: " + ep_name + " not in subscription list (OK).");
                    do_register = true;
                }
            }
            catch (Exception ex) {
                this.orchestrator().errorLogger().info("IoTEventHub: Exception in unsubscribe for " + ep_name + " : " + ex.getMessage());
            }
        }
        else {
            this.orchestrator().errorLogger().info("IoTEventHub: NULL Endpoint name in unsubscribe()... ignoring...");
        }
        return do_register;
    }
    
    private String getTopicElement(String topic,int index) {
        String element = "";
        String[] parsed = topic.split("/");
        if (parsed != null && parsed.length > index) 
            element = parsed[index];
        return element;
    }
    
    private String getEndpointNameFromTopic(String topic) {
        // format: iot-2/type/mbed/id/mbed-eth-observe/cmd/put/fmt/json
        return this.getTopicElement(topic,4);
    }
    
    private String getCoAPVerbFromTopic(String topic) {
        // format: iot-2/type/mbed/id/mbed-eth-observe/cmd/put/fmt/json
        return this.getTopicElement(topic, 6);
    }
    
    private String getCoAPURI(String message) {
        // expected format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe" }
        //this.errorLogger().info("getCoAPURI: payload: " + message);
        JSONParser parser = this.orchestrator().getJSONParser();
        Map parsed = parser.parseJson(message);
        return (String)parsed.get("path");
    }
    
    private String getCoAPValue(String message) {
        // expected format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe" }
        //this.errorLogger().info("getCoAPValue: payload: " + message);
        JSONParser parser = this.orchestrator().getJSONParser();
        Map parsed = parser.parseJson(message);
        return (String)parsed.get("new_value");
    }
    
    private String getCoAPEndpointName(String message) {
        // expected format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe" }
        //this.errorLogger().info("getCoAPValue: payload: " + message);
        JSONParser parser = this.orchestrator().getJSONParser();
        Map parsed = parser.parseJson(message);
        return (String)parsed.get("ep");
    }
    
    @Override
    public void onMessageReceive(String topic, String message) {
        // DEBUG
        this.errorLogger().info("IoTEventHub(CoAP Command): Topic: " + topic + " message: " + message);
        
        // parse the topic to get the endpoint and CoAP verb
        // format: iot-2/type/mbed/id/mbed-eth-observe/cmd/put/fmt/json
        String ep_name = this.getEndpointNameFromTopic(topic);
        String coap_verb = this.getCoAPVerbFromTopic(topic);
        
        // pull the CoAP URI and Payload from the message itself... its JSON... 
        // format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe" }
        String uri = this.getCoAPURI(message);
        String value = this.getCoAPValue(message);
        
        // if the ep_name is wildcarded... get the endpoint name from the JSON payload
        // format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe" }
        if (ep_name == null || ep_name.length() <= 0 || ep_name.equalsIgnoreCase("+")) {
            ep_name = this.getCoAPEndpointName(message);
        }
        
        // dispatch the coap resource operation request
        String response = this.orchestrator().processEndpointResourceOperation(coap_verb,ep_name,uri,value);
        
        // examine the response
        if (response != null && response.length() > 0) {
            // SYNC: We only process AsyncResponses from GET verbs... we dont sent HTTP status back through IoTEventHub.
            this.errorLogger().info("IoTEventHub(CoAP Command): Response: " + response);
            
            // AsyncResponse detection and recording...
            if (this.isAsyncResponse(response) == true) {
                if (coap_verb.equalsIgnoreCase("get") == true) {
                    // its an AsyncResponse.. so record it...
                    this.recordAsyncResponse(response,coap_verb,this.mqtt(),this,topic,message,uri,ep_name);
                }
                else {
                    // we ignore AsyncResponses to PUT,POST,DELETE
                    this.errorLogger().info("IoTEventHub(CoAP Command): Ignoring AsyncResponse for " + coap_verb + " (OK).");
                }
            }
            else if (coap_verb.equalsIgnoreCase("get")) {
                // not an AsyncResponse... so just emit it immediately... only for GET...
                this.errorLogger().info("IoTEventHub(CoAP Command): Response: " + response + " from GET... creating observation...");
                
                // we have to format as an observation...
                String observation = this.createObservation(coap_verb,ep_name,uri,response);
                
                // DEBUG
                this.errorLogger().info("IoTEventHub(CoAP Command): Sending Observation(GET): " + observation);
                
                // send the observation (GET reply)...
                // XXX this.mqtt().sendMessage(this.customizeTopic(this.m_iot_event_hub_observe_notification_topic,ep_name,this.m_iot_event_hub_device_manager.getDeviceType(ep_name)),observation,QoS.AT_MOST_ONCE); 
            }
        }
    }
    
    // create an observation JSON as a response to a GET request...
    private String createObservation(String verb, String ep_name, String uri, String value) {
        Map notification = new HashMap<>();
        
        // needs to look like this:  {"d":{"path":"/303/0/5700","payload":"MjkuNzU\u003d","max-age":"60","ep":"350e67be-9270-406b-8802-dd5e5f20ansond","value":"29.75"}}    
        notification.put("value", value);
        notification.put("path", uri);
        notification.put("ep",ep_name);
        
        // add a new field to denote its a GET
        notification.put("verb",verb);

        // we will send the raw CoAP JSON... IoTEventHub can parse that... 
        String coap_raw_json = this.jsonGenerator().generateJson(notification);

        // strip off []...
        String coap_json_stripped = this.stripArrayChars(coap_raw_json);

        // encapsulate into a coap/device packet...
        // XXX
        String iot_event_hub_coap_json = null;

        // DEBUG
        this.errorLogger().info("IoTEventHub: CoAP notification(GET REPLY): " + iot_event_hub_coap_json);
        
        // return the IoTEventHub-specific observation JSON...
        return iot_event_hub_coap_json;
    }
    
    // default formatter for AsyncResponse replies
    @Override
    public String formatAsyncResponseAsReply(Map async_response,String verb) {
        if (verb != null && verb.equalsIgnoreCase("GET") == true) {           
            try {
                // DEBUG
                this.errorLogger().info("IoTEventHub: CoAP AsyncResponse for GET: " + async_response);
                
                // get the Map of the response
                Map response_map = (Map)async_response.get("response_map");
                
                // Convert back to String, then to List
                String t = this.orchestrator().getJSONGenerator().generateJson(response_map);
                List async_responses = (List)this.orchestrator().getJSONParser().parseJson(t);
                for(int i=0;async_responses != null && i<async_responses.size();++i) {
                    // get the ith entry from the list
                    Map response = (Map)async_responses.get(i);
                    
                    // DEBUG
                    this.errorLogger().info("IoTEventHub: CoAP response(" + i + "): " + response);
                    
                    // get the payload from the ith entry
                    String payload = (String)response.get("payload");
                    if (payload != null) {
                        // trim 
                        payload = payload.trim();
                        
                        // parse if present
                        if (payload.length() > 0) {
                            // Base64 decode
                            String value = Utils.decodeCoAPPayload(payload);
                            
                            // build out the response
                            String uri = (String)async_response.get("uri");
                            String ep_name = (String)async_response.get("ep_name");
                            
                            // build out the 
                            String message = this.createObservation(verb, ep_name, uri, value);
                            
                            // DEBUG
                            this.errorLogger().info("IoTEventHub: Created(" + verb + ") GET Observation: " + message);
                            
                            // return the message
                            return message;
                        }
                    }
                }
            }
            catch (Exception ex) {
                // Error in creating the observation message from the AsyncResponse GET reply... 
                this.errorLogger().warning("formatAsyncResponseAsReply(IoTEventHub): Exception during GET reply -> observation creation. Not sending GET as observation...",ex);
            }
        }
        return null;
    }
    
    // process new device registration
    @Override
    protected Boolean registerNewDevice(Map message) {
        if (this.m_iot_event_hub_device_manager != null) {
            return this.m_iot_event_hub_device_manager.registerNewDevice(message);
        }
        return false;
    }
    
    // process device de-registration
    @Override
    protected Boolean deregisterDevice(String device) {
        if (this.m_iot_event_hub_device_manager != null) {
            return this.m_iot_event_hub_device_manager.deregisterDevice(device);
        }
        return false;
    }
}
