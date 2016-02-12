/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.arm.connector.bridge.json;

import java.util.Map;

/**
 *
 * @author Doug Anson
 */
public class JSONParser {
    // default constructor
    public JSONParser() {
    }
    
    // parse JSON into Map/List 
    public Map parseJson(String json) {
        return com.codesnippets4all.json.parsers.JsonParserFactory.getInstance().newJsonParser().parseJson(json);
    }
}
