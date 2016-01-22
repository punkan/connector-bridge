/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.arm.connector.bridge.transport;

import org.fusesource.mqtt.client.Message;

/**
 *
 * @author Doug Anson
 */
public class MQTTMessage {
    Message m_mqtt_message;
    String m_message;
    String m_topic;
    
    public MQTTMessage(Message mqtt_message) {
        this.m_mqtt_message = mqtt_message;
        byte[] payload = this.m_mqtt_message.getPayload();
        if (payload != null && payload.length > 0) {
            this.m_message = new String(payload);
        }
        this.m_topic = this.m_mqtt_message.getTopic();
    }
    
    public String getMessage() { return this.m_message; }
    public String getTopic() { return this.m_topic; }
    public void ack() { this.m_mqtt_message.ack(); }
}
