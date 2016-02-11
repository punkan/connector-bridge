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
public class JSONGenerator {
    private com.codesnippets4all.json.generators.JSONGenerator m_generator = null;
    
    // default constructor
    public JSONGenerator() {
        this.m_generator = com.codesnippets4all.json.generators.JsonGeneratorFactory.getInstance().newJsonGenerator();
    }
    
    // create JSON
    public String generateJson(Object json) {
        return this.m_generator.generateJson(json);
    }
}
