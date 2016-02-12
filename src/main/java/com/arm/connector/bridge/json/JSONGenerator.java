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
    // default constructor
    public JSONGenerator() {
    }
    
    // create JSON
    public String generateJson(Object json) {
        return com.codesnippets4all.json.generators.JsonGeneratorFactory.getInstance().newJsonGenerator().generateJson(json);
    }
}
