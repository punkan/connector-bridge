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

/**
 * Receive Thread for message traffic inbound
 *
 * @author Doug Anson
 */
public class TransportReceiveThread extends Thread implements Transport.ReceiveListener {

    private boolean m_running = false;
    private Transport m_transport = null;
    private Transport.ReceiveListener m_listener = null;

    /**
     * Constructor
     * @param mqtt
     */
    public TransportReceiveThread(Transport mqtt) {
        this.m_transport = mqtt;
        this.m_transport.setOnReceiveListener(this);
        this.m_running = false;
        this.m_listener = null;
    }

    /**
     * get our running state
     * @return
     */
    public boolean isRunning() {
        return this.m_running;
    }

    /**
     * get our connection status
     * @return
     */
    public boolean isConnected() {
        return this.m_transport.isConnected();
    }

    /**
     * disconnect
     */
    public void disconnect() {
        if (this.m_transport != null) {
            this.m_transport.disconnect();
        }
    }

    /**
     * run method for the receive thread
     */
    @Override
    public void run() {
        if (!this.m_running) {
            this.m_running = true;
            this.listenerThreadLoop();
        }
    }

    /**
     * main thread loop
     */
    @SuppressWarnings("empty-statement")
    private void listenerThreadLoop() {
        int sleep_time = ((this.m_transport.preferences().intValueOf("mqtt_receive_loop_sleep"))*1000);
        while (this.m_running && this.m_transport.isConnected() == true) {
            // receive and process...
            this.m_transport.receiveAndProcess();
            
            // sleep for a bit...
            try {
                Thread.sleep(sleep_time);
            }
            catch(InterruptedException ex) {
                // silent
                ;
            }
        }
    }

    /**
     * set the receive listener
     * @param listener
     */
    public void setOnReceiveListener(Transport.ReceiveListener listener) {
        this.m_listener = listener;
        this.m_transport.setOnReceiveListener(this);
    }

    /**
     * callback on Message Receive events
     * @param message
     */
    @Override
    public void onMessageReceive(String topic, String message) {
        if (this.m_listener != null) {
            this.m_listener.onMessageReceive(topic, message);
        }
    }
}
