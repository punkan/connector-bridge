/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.arm.connector.bridge.coordinator.processors.core;

/**
 *
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
