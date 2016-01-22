/* Copyright (C) 2013 ARM
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software
 * and associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.arm.connector.bridge.transport;

import com.arm.connector.bridge.core.BaseClass;
import com.arm.connector.bridge.core.ErrorLogger;
import com.arm.connector.bridge.preferences.PreferenceManager;

/**
 * Generic Transport
 *
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
