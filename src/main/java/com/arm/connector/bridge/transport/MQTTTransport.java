/**
 * @file    MQTTTransport.java
 * @brief   MQTT Transport Support 
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

package com.arm.connector.bridge.transport;

import com.arm.connector.bridge.core.Transport;
import com.arm.connector.bridge.core.ErrorLogger;
import com.arm.connector.bridge.core.Utils;
import com.arm.connector.bridge.preferences.PreferenceManager;
import java.io.EOFException;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

/**
 * MQTT Transport Support
 *
 * @author Doug Anson
 */
public class MQTTTransport extends Transport {

    private static MQTTTransport m_self = null;
    private BlockingConnection m_connection = null;
    private byte[] m_qoses = null;
    private String m_suffix = null;
    private String m_username = null;
    private String m_password = null;
    private String m_host_url = null;
    private int m_sleep_time = 0; 
    private String m_client_id = null;
 
    private String m_connect_host = null;
    private int m_connect_port = 0;
    private String m_connect_client_id = null;
    private boolean m_connect_clean_session = false;
    
    private int m_num_retries = 0;
    private int m_max_retries = 10;
    private Topic[] m_subscribe_topics = null;
    private String[] m_unsubscribe_topics = null;
    
    /**
     * Instance Factory
     * @param error_logger
     * @param preference_manager
     * @return
     */
    public static Transport getInstance(ErrorLogger error_logger, PreferenceManager preference_manager) {
        if (MQTTTransport.m_self == null) // create our MQTT transport
        {
            MQTTTransport.m_self = new MQTTTransport(error_logger, preference_manager);
        }
        return MQTTTransport.m_self;
    }
    
    /**
     * Constructor
     * @param error_logger
     * @param preference_manager
     * @param suffix
     */
    public MQTTTransport(ErrorLogger error_logger, PreferenceManager preference_manager,String suffix) {
        this(error_logger,preference_manager);
        this.m_suffix = suffix;
        this.setUsername(this.prefValue("mqtt_username",this.m_suffix));
        this.setPassword(this.prefValue("mqtt_password",this.m_suffix));
        this.m_host_url = null;
        this.m_max_retries = (this.preferences().intValueOf("mqtt_connect_retries",this.m_suffix));
        this.m_sleep_time = ((this.preferences().intValueOf("mqtt_receive_loop_sleep",this.m_suffix))*1000);
    }

    /**
     * Constructor
     * @param error_logger
     * @param preference_manager
     */
    public MQTTTransport(ErrorLogger error_logger, PreferenceManager preference_manager) {
        super(error_logger, preference_manager);
        this.m_suffix = null;
    }
     
    // PUBLIC: Create the authentication hash
    public String createAuthenticationHash() {
        return Utils.createHash(this.getUsername() + "_" + this.getPassword() + "_" + this.prefValue("mqtt_client_id",this.m_suffix));
    }
        
    // PRIVATE: Username/PW for MQTT connection
    private String getUsername() {
        return this.m_username;
    }

    // PRIVATE: Username/PW for MQTT connection
    private String getPassword() {
        return this.m_password;
    }
    
    // PUBLIC: Get the client ID
    public String getClientID() {
        return this.m_client_id;
    }
    
    // PUBLIC: Set the client ID
    public void setClientID(String clientID) {
        this.m_client_id = clientID;
    }
    
     /**
     * Set the MQTT Username
     * @param username
     */
    public final void setUsername(String username) {
        this.m_username = username;
    }

    /**
     * Set the MQTT Password
     * @param password
     */
    public final void setPassword(String password) {
        this.m_password = password;
    }

    /**
     * Are we connected to a MQTT broker?
     * @return
     */
    @Override
    public boolean isConnected() {
        if (this.m_connection != null) {
            return this.m_connection.isConnected();
        }
        //this.errorLogger().warning("WARNING: MQTT connection instance is NULL...");
        return super.isConnected();
    }
    
    /**
     * Connect to the MQTT broker
     * @param host
     * @param port
     * @return
     */
    @Override
    public boolean connect(String host, int port) {
        return this.connect(host,port,this.prefValue("mqtt_client_id",this.m_suffix), this.prefBoolValue("mqtt_clean_session",this.m_suffix));
    }
    
    /**
     * Connect to the MQTT broker
     * @param host
     * @param port
     * @param clientID
     * @return
     */
    public boolean connect(String host, int port, String clientID) {
        return this.connect(host,port,clientID,this.prefBoolValue("mqtt_clean_session",this.m_suffix));
    }
    /**
     * Connect to the MQTT broker
     * @param host
     * @param port
     * @param clientID
     * @param clean_session
     * @return
     */
    public boolean connect(String host, int port, String clientID,boolean clean_session) {
        boolean connected = this.isConnected();
        int sleep_time = this.prefIntValue("mqtt_retry_sleep",this.m_suffix);
        int num_tries = this.prefIntValue("mqtt_connect_retries",this.m_suffix);
        for(int i=0;i<num_tries && !connected; ++i) {
            try {                
                // MQTT endpoint 
                MQTT endpoint = new MQTT();
                
                // build out the URL connection string
                String url = this.setupHostURL(host, port);
                
                // setup default clientID
                if (clientID == null || clientID.length() <= 0) {
                    clientID = this.prefValue("mqtt_client_id",this.m_suffix);
                }
                
                // DEBUG
                this.errorLogger().info("MQTTTransport: URL: [" + url + "] clientID: [" + clientID + "] clean: " + clean_session);
 
                // setup the hostname & port
                endpoint.setHost(url);
                
                // set the MQTT version
                String mqtt_version = this.prefValue("mqtt_version",this.m_suffix);
                if (mqtt_version != null) {
                    endpoint.setVersion(mqtt_version);
                }
                
                // configure credentials and options...
                String username = this.getUsername();
                String pw = this.getPassword();
                
                // DEBUG
                this.errorLogger().info("MQTTTransport: Username: [" + username + "] pw: [" + pw + "]");
                
                if (username != null && username.length() > 0 && username.equalsIgnoreCase("off") == false) {
                    endpoint.setUserName(username);
                }
                if (pw != null && pw.length() > 0 && pw.equalsIgnoreCase("off") == false) {
                    endpoint.setPassword(pw);
                }
                if (clientID != null && clientID.length() > 0 && clientID.equalsIgnoreCase("off") == false) {
                    endpoint.setClientId(clientID);
                }
                else if (clean_session == false) {
                    String def_client_id = this.prefValue("mqtt_default_client_id",this.m_suffix);
                    if (def_client_id != null && def_client_id.equalsIgnoreCase("off") == false) {
                        // set a defaulted clientID
                        endpoint.setClientId(def_client_id);
                    }
                }
                endpoint.setCleanSession(clean_session);
                String will = this.prefValue("mqtt_will_message",this.m_suffix);
                if (will != null && will.length() > 0 && will.equalsIgnoreCase("off") == false) {
                    endpoint.setWillMessage(will);
                }
                String will_topic = this.prefValue("mqtt_will_topic",this.m_suffix);
                if (will_topic != null && will_topic.length() > 0 && will_topic.equalsIgnoreCase("off") == false) {
                    endpoint.setWillTopic(will_topic);
                }
                int trafficClass = this.prefIntValue("mqtt_traffic_class",this.m_suffix);
                if (trafficClass >= 0) {
                    endpoint.setTrafficClass(trafficClass);
                }
                int reconnectAttempts = this.prefIntValue("mqtt_reconnect_retries_max",this.m_suffix);
                if (reconnectAttempts >= 0) {
                    endpoint.setReconnectAttemptsMax(reconnectAttempts);
                }
                long reconnectDelay = (long)this.prefIntValue("mqtt_reconnect_delay",this.m_suffix);
                if (reconnectDelay >= 0) {
                    endpoint.setReconnectDelay(reconnectDelay);
                }
                long reconnectDelayMax = (long)this.prefIntValue("mqtt_reconnect_delay_max",this.m_suffix);
                if (reconnectDelayMax >= 0) {
                    endpoint.setReconnectDelayMax(reconnectDelayMax);
                }
                float backoffMultiplier = this.prefFloatValue("mqtt_backoff_multiplier",this.m_suffix);
                if (backoffMultiplier >= 0) {
                    endpoint.setReconnectBackOffMultiplier(backoffMultiplier);
                }
                short keepAlive = (short)this.prefIntValue("mqtt_keep_alive",this.m_suffix);
                if (keepAlive >= 0) {
                    endpoint.setKeepAlive(keepAlive);
                }
                if (endpoint.getClientId() != null) {
                    this.m_client_id = endpoint.getClientId().toString();
                }
                
                try {
                    // connect MQTT...
                    this.m_endpoint = endpoint;
                    this.m_connection = endpoint.blockingConnection();
                    if (this.m_connection != null) {
                        // attempt connection
                        this.m_connection.connect();

                        // sleep for a short bit...
                        try {
                            Thread.sleep(sleep_time);
                        }
                        catch (InterruptedException ex) {
                            this.errorLogger().critical("MQTTTransport(connect): sleep interrupted",ex);
                        }

                        // check our connection status
                        connected = this.m_connection.isConnected();

                        // DEBUG
                        if (connected == true) {
                            this.errorLogger().warning("MQTTTransport: Connection to: " + url + " successful");
                            this.m_connect_host = host;
                            this.m_connect_port = port;
                            this.m_client_id = endpoint.getClientId().toString();
                            this.m_connect_client_id = this.m_client_id;
                            this.m_connect_clean_session = clean_session;
                        }
                        else {
                            this.errorLogger().warning("MQTTTransport: Connection to: " + url + " FAILED");
                        }
                    }
                    else {
                        this.errorLogger().warning("WARNING: MQTT connection instance is NULL. connect() failed");
                    }
                }
                catch (Exception ex) {
                    this.errorLogger().warning("MQTTTransport: Exception during connect()",ex);
                    
                    // DEBUG
                    this.errorLogger().warning("MQTT: URL: " + url);
                    this.errorLogger().warning("MQTT: clientID: " + this.m_client_id);
                    this.errorLogger().warning("MQTT: clean_session: " + clean_session);
                    this.errorLogger().warning("MQTT: username: " + username);
                    this.errorLogger().warning("MQTT: password: " + pw);
                    this.errorLogger().warning("MQTT: host: " + host);
                    this.errorLogger().warning("MQTT: port: " + port);
                    
                    // sleep for a short bit...
                    try {
                        Thread.sleep(sleep_time);
                    }
                    catch (InterruptedException ex2) {
                        this.errorLogger().critical("MQTTTransport(connect): sleep interrupted",ex2);
                    }
                }
            }
            catch (Exception ex) {
                this.errorLogger().critical("MQTTTransport(connect): Exception occured", ex);
                this.m_connected = false;
            }
            
            // record our status
            this.m_connected = connected;
            
            // if we have not yet connected... sleep a bit more and retry...
            if (!this.isConnected() == false) {
                try {
                    Thread.sleep(sleep_time);
                }
                catch (InterruptedException ex) {
                    this.errorLogger().critical("MQTTTransport(retry): sleep interrupted",ex);
                }
            }
        }
        
        // return our connection status
        return this.isConnected();
    }

    /**
     * Main handler for receiving and processing MQTT Messages (called repeatedly by TransportReceiveThread...)
     * @return true - processed (or empty), false - failure
     */
    @Override
    public boolean receiveAndProcess() {
        // DEBUG
        //this.errorLogger().info("MQTTTransport: in receiveAndProcess()...");
        if (this.isConnected()) {
            try {
                // receive the MQTT message and process it...
                this.errorLogger().info("MQTTTransport: in receiveAndProcess(). Calling receiveAndProcessMessage()...");
                this.receiveAndProcessMessage();
            }
            catch (Exception ex) {
                // note
                this.errorLogger().info("MQTTTransport: caught Exception in recieveAndProcess(): " + ex.getMessage());
                return false;
            }
            return true;
        }
        else {
            this.errorLogger().info("MQTTTransport: not connected (OK)");
            return true;
        }
    }
    
    // reset our MQTT connection... sometimes it goes wonky...
    private void resetConnection() {
        try {
            // disconnect()...
            ++this.m_num_retries;
            this.disconnect(false);
            
            // sleep a bit
            Thread.sleep(this.m_sleep_time);
            
            // reconnect()...
            if (this.reconnect() == true) {
                // DEBUG
                this.errorLogger().info("resetConnection: SUCCESS.");
                    
                // reconnected OK...
                this.m_num_retries = 0;
                
                // resubscribe
                if (this.m_subscribe_topics != null) {
                    // DEBUG
                    this.errorLogger().info("resetConnection: SUCCESS. re-subscribing...");
                    this.subscribe(this.m_subscribe_topics);
                }
            }
            else {
                // DEBUG
                this.errorLogger().info("resetConnection: FAILURE num_tries = " + this.m_num_retries);
            }
        }
        catch (Exception ex) {
            this.errorLogger().info("resetConnection: Exception: " + ex.getMessage(),ex);
        }
    }
    
    // have we exceeded our retry count?
    private Boolean retriesExceeded() {
        return (this.m_num_retries >= this.m_max_retries);
    }

    // subscribe to specific topics 
    public void subscribe(Topic[] list) {
        if (this.m_connection != null) {
            try {
                this.m_subscribe_topics = list;
                this.m_qoses = this.m_connection.subscribe(list);
                //this.errorLogger().info("MQTTTransport: Subscribed to TOPIC(s): " + list.length);
            }
            catch (Exception ex) {
                if (this.retriesExceeded()) {
                    // unable to subscribe to topic (final)
                    this.errorLogger().critical("MQTTTransport: unable to subscribe to topic (final)", ex);
                }
                else {
                    // unable to subscribe to topic
                    this.errorLogger().warning("MQTTTransport: unable to subscribe to topic (" + this.m_num_retries + " of " + this.m_max_retries + ")", ex);

                    // attempt reset
                    this.resetConnection();
                }
            }
        }
        else {
            // unable to subscribe - not connected... 
            this.errorLogger().warning("MQTTTransport: unable to subscribe. Connection is missing and/or NULL");
        }
    }
    
    // unsubscribe from specific topics
    public void unsubscribe(String[] list) {
        if (this.m_connection != null) {
            try {
                this.m_unsubscribe_topics = null;
                this.m_connection.unsubscribe(list);
                //this.errorLogger().info("MQTTTransport: Unsubscribed from TOPIC(s): " + list.length);
            }
            catch (Exception ex) {
                if (this.retriesExceeded()) {
                    // unable to unsubscribe from topic (final)
                    this.errorLogger().critical("MQTTTransport: unable to unsubscribe from topic (final)", ex);
                }
                else {
                               // unable to subscribe to topic
                    this.errorLogger().warning("MQTTTransport: unable to unsubscribe to topic (" + this.m_num_retries + " of " + this.m_max_retries + ")", ex);

                    // attempt reset
                    this.resetConnection();

                    // recall
                    this.unsubscribe(list);
                }
            }
        }
        else {
            // unable to subscribe - not connected... 
            this.errorLogger().warning("MQTTTransport: unable to unsubscribe. Connection is missing and/or NULL");
        }
    }

    /**
     * Publish a MQTT message
     * @param topic
     * @param message
     */
    @Override
    public void sendMessage(String topic,String message) {
        this.sendMessage(topic,message,QoS.AT_LEAST_ONCE);
    }

    /**
     * Publish a MQTT message
     * @param topic
     * @param message
     * @param qos
     * @return send status
     */
    public boolean sendMessage(String topic,String message,QoS qos) {
        boolean sent = false;
        if (this.isConnected() && message != null) {
            try {
                //this.errorLogger().info("MQTT: Sending message: " + message + " Topic: " + topic);
                this.m_connection.publish(topic, message.getBytes(), qos, false);
                sent = true;
            }
            catch (EOFException ex) {
                if (this.retriesExceeded()) {
                    // unable to send (EOF) - final
                    this.errorLogger().critical("sendMessage:EOF on message send... resetting MQTT (final): " + message, ex);
                }
                else {
                    // unable to send (EOF) - final
                    this.errorLogger().warning("sendMessage:EOF on message send... resetting MQTT (" + this.m_num_retries + " of " + this.m_max_retries + "): " + message, ex);

                    // reset the connection
                    this.resetConnection();

                    // resend
                    if (this.isConnected()) {
                        this.errorLogger().info("sendMessage: retrying send() after EOF/reconnect....");
                        sent = this.sendMessage(topic,message,qos);
                    }
                    else {
                        // unable to send (not connected)
                        this.errorLogger().warning("sendMessage: NOT CONNECTED after EOF/reconnect. Unable to send message: " + message);
                    }
                }
            }
            catch (Exception ex) {
                // unable to send (general fault)
                this.errorLogger().critical("sendMessage: unable to send message: " + message, ex);
            }
        }
        else {
            // unable to send (not connected)
            this.errorLogger().warning("sendMessage: NOT CONNECTED. Unable to send message: " + message);
        }
        
        // return the status
        return sent;
    }
    
    // get the next MQTT message
    private MQTTMessage getNextMessage() throws Exception {
        MQTTMessage message = null;
        message = new MQTTMessage(this.m_connection.receive());
        message.ack();
        return message;
    }

    /**
     * Receive and process a MQTT Message
     * @return
     */
    public MQTTMessage receiveAndProcessMessage() {
        MQTTMessage message = null;
        try {
            // DEBUG
            //this.errorLogger().info("receiveMessage: getting next MQTT message...");
            message = this.getNextMessage();
            if (this.m_listener != null && message != null) {
                // call the registered listener to process the received message
                this.errorLogger().info("receiveMessage: processing message: " + message);
                this.m_listener.onMessageReceive(message.getTopic(),message.getMessage());
            }
            else if (this.m_listener != null) {
                // no listener
                this.errorLogger().critical("receiveMessage: Not processing message: " + message + ". Listener is NULL");
            }
            else {
                // no message
                this.errorLogger().critical("receiveMessage: Not processing NULL message");
            }
        }
        catch (Exception ex) {
            if (this.retriesExceeded()) {
                // unable to receiveMessage - final
                this.errorLogger().critical("receiveMessage: unable to receive message (final)", ex);
            }
            else {
                // unable to receiveMessage - final
                this.errorLogger().critical("receiveMessage: unable to receive message (" + this.m_num_retries + " of " + this.m_max_retries + "): " + ex.getMessage(), ex);

                // reset the connection
                this.resetConnection();

                // re-receive
                if (this.isConnected()) {
                    this.errorLogger().info("receiveMessage: retrying receive() after EOF/reconnect...");
                    this.receiveAndProcessMessage();
                }
            }
        }
        return message;
    }

    /**
     * Disconnect from MQTT broker
     */
    @Override
    public void disconnect() {
        this.disconnect(true);
    }
    
    // Disconnect from MQTT broker
    public void disconnect(boolean clear_creds) {
        // disconnect... 
        try {
            if (this.m_connection != null) {
                this.m_connection.disconnect();
            }
        }
        catch (Exception ex) {
            // unable to send
            this.errorLogger().critical("disconnect: exception during disconnect(). ", ex);
        }
        
        // clean up...
        super.disconnect();
        this.m_endpoint = null;
        this.m_connection = null;
        
        // clear the cached creds 
        if (clear_creds == true) {
            this.m_connect_host = null;
            this.m_connect_port = 0;
            this.m_connect_client_id = null;
            this.m_connect_clean_session = false;
        }
    }
    
    private boolean reconnect() {
        if (this.m_connect_host != null) {
            // attempt reconnect with cached creds...
            return this.connect(this.m_connect_host,this. m_connect_port, this.m_connect_client_id, this.m_connect_clean_session);
        }
        else {
            // no initial connect() has succeeded... so no cached creds available
            this.errorLogger().warning("reconnect: unable to reconnect() prior to initial connect() success...");
            return false;
        }
    }
    
    // setup the MQTT host URL
    private String setupHostURL(String host, int port) {
        if (this.m_host_url == null) {
            boolean secured = this.prefBoolValue("mqtt_use_ssl",this.m_suffix);
            
            // PREFIX determination
            String prefix = "tcp://";
            if (secured) {
                prefix = "ssl://";
                port += 7000;           // 1883 --> 8883
            }
            this.m_host_url = prefix + host + ":" + port;
        }
        return this.m_host_url;
    }
}
