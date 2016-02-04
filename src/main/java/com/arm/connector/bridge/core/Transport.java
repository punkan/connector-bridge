/**
 * @file    Transport.java
 * @brief   transport base class 
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

package com.arm.connector.bridge.core;

import com.arm.connector.bridge.preferences.PreferenceManager;

/**
 * Generic transport base class
 * @author Doug Anson
 */
public abstract class Transport extends BaseClass {

    /**
     *
     */
    protected boolean m_connected = false;
    private final ErrorLogger m_error_logger = null;
    private final PreferenceManager m_preference_manager = null;

    /**
     *
     */
    protected Object m_endpoint = null;

    /**
     *
     */
    protected Transport.ReceiveListener m_listener = null;

    // Interface for receive listener callbacks
    /**
     *
     */
    public interface ReceiveListener {
        // on message receive, this will be callback to the registered listener

        /**
         *
         * @param topic
         * @param message
         */
        public void onMessageReceive(String topic, String message);
    }

    // Constructor
    /**
     *
     * @param error_logger
     * @param preference_manager
     */
    public Transport(ErrorLogger error_logger, PreferenceManager preference_manager) {
        super(error_logger, preference_manager);
        this.m_connected = false;
    }

    // main thread loop
    /**
     *
     */
    public abstract boolean receiveAndProcess();

    // connect transport
    /**
     *
     * @param host
     * @param port
     * @return
     */
    public abstract boolean connect(String host, int port);

    // send a message
    /**
     *
     * @param topic
     * @param message
     */
    public abstract void sendMessage(String topic,String message);

    // disconnect
    /**
     *
     */
    public void disconnect() {
        this.m_connected = false;
    }

    // set the receive listener
    /**
     *
     * @param listener
     */
    public void setOnReceiveListener(Transport.ReceiveListener listener) {
        this.m_listener = listener;
    }

    // connection status
    /**
     *
     * @return
     */
    public boolean isConnected() {
        return this.m_connected;
    }
}
