/**
 * @file    Processor.java
 * @brief   peer processor base class
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

import com.arm.connector.bridge.coordinator.Orchestrator;
import com.arm.connector.bridge.core.BaseClass;
import com.codesnippets4all.json.generators.JSONGenerator;
import com.codesnippets4all.json.parsers.JSONParser;

/**
 * Peer Processor base class
 * @author Doug Anson
 */
public class Processor extends BaseClass {
    private static final String DEFAULT_EMPTY_STRING = "0";
    private Orchestrator     m_orchestrator = null;
    private JSONGenerator    m_json_generator = null;
    private JSONParser       m_json_parser = null;
    protected String         m_mds_domain = null;
    private String           m_def_domain = null;
    private String           m_empty_string = Processor.DEFAULT_EMPTY_STRING;
    
    public Processor(Orchestrator orchestrator,String suffix) {
        super(orchestrator.errorLogger(),orchestrator.preferences());
        this.m_orchestrator = orchestrator;
        this.m_def_domain = orchestrator.preferences().valueOf("mds_def_domain",suffix);
        this.m_json_parser = orchestrator.getJSONParser();
        this.m_json_generator = orchestrator.getJSONGenerator();
        this.m_empty_string = orchestrator.preferences().valueOf("mds_bridge_empty_string",suffix);
        if (this.m_empty_string == null) this.m_empty_string = Processor.DEFAULT_EMPTY_STRING;
    }
    
    // create the authentication hash
    protected String createAuthenticationHash() {
        return this.orchestrator().createAuthenticationHash();
    }
   
    // jsonParser is broken with empty strings... so we have to fill them in with spaces.. 
    private String replaceEmptyStrings(String data) {
        if (data != null) return data.replaceAll("\"\"", "\"" + this.m_empty_string + "\"");
        return data;
    }
    
    // parse the JSON...
    protected Object parseJson(String json) {
        Object parsed = null;
        String modified_json = "";
        try {
            modified_json = this.replaceEmptyStrings(json);
            if (json != null && json.contains("{") && json.contains("}"))
                parsed = this.jsonParser().parseJson(modified_json);
        }
        catch (Exception ex) {
            this.orchestrator().errorLogger().critical("JSON parsing exception for: " + modified_json + " message: " + ex.getMessage(),ex);
            parsed = null;
        }
        return parsed;
    }
    
    // protected getters/setters...
    protected JSONParser jsonParser() { return this.m_json_parser; }
    protected JSONGenerator jsonGenerator() { return this.m_json_generator; }
    protected Orchestrator orchestrator() { return this.m_orchestrator; }
    protected void setDomain(String mds_domain) { this.m_mds_domain = mds_domain; }
    protected String getDomain() { return this.getDomain(false); }
    protected String getDomain(boolean show_default) {
        if (this.m_mds_domain != null) return "/" + this.m_mds_domain; 
        if (show_default == true) return "/" + this.m_def_domain;
        return "";
    }
}
