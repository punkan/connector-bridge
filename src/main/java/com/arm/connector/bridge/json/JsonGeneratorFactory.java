/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.arm.connector.bridge.json;

/**
 *
 * @author Doug Anson
 */
public class JsonGeneratorFactory {
    private static JsonGeneratorFactory me = null;
        
    // JSON Generator and Parser
    private JSONGenerator m_generator = null;
    private JSONParser m_parser = null;
    
    // generator
    public static JsonGeneratorFactory getInstance() {
        if (me == null) {
            me = new JsonGeneratorFactory();
        }
        return me;
    }
    
    // constructor
    public JsonGeneratorFactory() {
        this.m_generator = new JSONGenerator();
        this.m_parser = new JSONParser();
    }
    
    // create a json generator
    public JSONGenerator newJsonGenerator() {
        return this.m_generator;
    }
    
    // create a json parser
    public JSONParser newJsonParser() {
        return this.m_parser;
    }
}
