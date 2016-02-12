/**
 * @file    MQTTTransport.java
 * @brief   MQTT Transport Support 
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
                this.errorLogger().info("MQTTTransport: URL: [" + url + "] clientID: [" + clientID + "]");
 
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
                    // set a defaulted clientID
                    endpoint.setClientId(this.prefValue("mqtt_default_client_id",this.m_suffix));
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
                            this.errorLogger().info("MQTTTransport: Connection to: " + url + " successful");
                            this.m_connect_host = host;
                            this.m_connect_port = port;
                            this.m_client_id = endpoint.getClientId().toString();
                            this.m_connect_client_id = this.m_client_id;
                            this.m_connect_clean_session = clean_session;
                        }
                        else {
                            this.errorLogger().info("MQTTTransport: Connection to: " + url + " FAILED");
                        }
                    }
                    else {
                        this.errorLogger().warning("WARNING: MQTT connection instance is NULL. connect() failed");
                    }
                }
                catch (Exception ex) {
                    this.errorLogger().info("MQTTTransport: Exception during connect()",ex);
                    
                    // DEBUG
                    this.errorLogger().info("MQTT: URL: " + url);
                    this.errorLogger().info("MQTT: clientID: " + this.m_client_id);
                    this.errorLogger().info("MQTT: clean_session: " + clean_session);
                    this.errorLogger().info("MQTT: username: " + username);
                    this.errorLogger().info("MQTT: password: " + pw);
                    this.errorLogger().info("MQTT: host: " + host);
                    this.errorLogger().info("MQTT: port: " + port);
                    
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
        if (this.isConnected()) {
            try {
                // receive the MQTT message and process it...
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

    // subscribe to specific topics 
    public void subscribe(Topic[] list) {
        try {
            this.m_qoses = this.m_connection.subscribe(list);
            //this.errorLogger().info("MQTTTransport: Subscribed to TOPIC(s): " + list.length);
        }
        catch (Exception ex) {
            // unable to subscribe to topic
            this.errorLogger().critical("MQTTTransport: unable to subscribe to topic", ex);
        }
    }
    
    // unsubscribe from specific topics
    public void unsubscribe(String[] list) {
        try {
            this.m_connection.unsubscribe(list);
            //this.errorLogger().info("MQTTTransport: Unsubscribed from TOPIC(s): " + list.length);
        }
        catch (Exception ex) {
            // unable to subscribe to topic
            this.errorLogger().critical("MQTTTransport: unable to unsubscribe from topic", ex);
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
     */
    public void sendMessage(String topic,String message,QoS qos) {
        if (this.isConnected() && message != null) {
            try {
                //this.errorLogger().info("MQTT: Sending message: " + message + " Topic: " + topic);
                this.m_connection.publish(topic, message.getBytes(), qos, false);
            }
            catch (EOFException ex) {
                // unable to send (EOF)
                this.errorLogger().warning("sendMessage:EOF on message send... resetting MQTT: " + message, ex);
                
                // disconnect
                this.disconnect(false);
                
                // reconnect
                this.reconnect();
                
                // resend
                if (this.isConnected()) {
                    this.errorLogger().info("sendMessage: retrying send() after EOF/reconnect....");
                    this.sendMessage(topic,message,qos);
                }
            }
            catch (Exception ex) {
                // unable to send (general fault)
                this.errorLogger().critical("sendMessage: unable to send message: " + message, ex);
            }
        }
    }
    
    // get the next MQTT message
    private MQTTMessage getNextMessage() {
        MQTTMessage message = null;
        try {
            message = new MQTTMessage(this.m_connection.receive());
            message.ack();
        }
        catch (InterruptedException ex) {
            // disconnect
            //this.errorLogger().info("disconnecting MQTT transport");
            this.disconnect();
        }
        catch (Exception ex) {
            this.errorLogger().warning("getNextMessage: exception during MQTT message get", ex);
        }
        return message;
    }

    /**
     * Receive and process a MQTT Message
     * @return
     */
    public MQTTMessage receiveAndProcessMessage() {
        MQTTMessage message = null;
        if (this.isConnected()) {
            try {
                message = this.getNextMessage();
                if (this.m_listener != null && message != null) {
                    this.m_listener.onMessageReceive(message.getTopic(),message.getMessage());
                }
            }
            catch (Exception ex) {
                // unable to receive
                this.errorLogger().critical("receiveMessage: unable to receive message", ex);
            }
        }
        else {
            try {
                // disconnect but keep cached creds...
                this.disconnect(false);
                
                // sleep a bit
                Thread.sleep(this.m_sleep_time);
                
                // try to reconnect
                if (this.reconnect() == true) {
                    // call again to see if we can retrieve any messages...
                    return this.receiveAndProcessMessage();
                }
                else {
                    // unable to receive
                    this.errorLogger().warning("receiveMessage: reconnect() FAILED... retrying...");
                }
            }
            catch (InterruptedException ex) {
                // unable to receive
                this.errorLogger().warning("receiveMessage: unable to re-connect... Exception: " + ex.getMessage());
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
                prefix = "tls://";
                port += 7000;           // 1883 --> 8883
            }
            this.m_host_url = prefix + host + ":" + port;
        }
        return this.m_host_url;
    }
}
