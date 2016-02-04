/**
 * @file    Message.java
 * @brief   message base class 
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

package com.arm.connector.bridge.coordinator.processors.core;

/**
 * message base class
 * @author Doug Anson
 */
public class Message {
    private String m_uri = null;
    private String m_content = null;
    private boolean m_wait = false;
    
    public Message(String uri,String content,boolean wait) {
        this.m_uri = uri;
        this.m_content = content;
        this.m_wait = wait;
    }
    
    public String uri() { return this.m_uri; }
    public String content() { return this.m_content; }
    public boolean waitForResponse() { return this.m_wait; }
}
