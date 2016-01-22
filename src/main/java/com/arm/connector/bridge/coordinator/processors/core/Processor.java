/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.arm.connector.bridge.coordinator.processors.core;

import com.arm.connector.bridge.coordinator.Orchestrator;
import com.arm.connector.bridge.core.BaseClass;
import com.codesnippets4all.json.exceptions.JSONParsingException;
import com.codesnippets4all.json.generators.JSONGenerator;
import com.codesnippets4all.json.parsers.JSONParser;

/**
 *
 * @author Doug Anson
 */
public class Processor extends BaseClass {
    private Orchestrator        m_manager = null;
    private JSONGenerator           m_json_generator = null;
    private JSONParser              m_json_parser = null;
    protected String                m_mds_domain = null;
    private String                  m_def_domain = null;
    
    public Processor(Orchestrator manager,String suffix) {
        super(manager.errorLogger(),manager.preferences());
        this.m_manager = manager;
        this.m_def_domain = manager.preferences().valueOf("mds_def_domain",suffix);
        this.m_json_parser = manager.getJSONParser();
        this.m_json_generator = manager.getJSONGenerator();
    }
   
    // jsonParser is broken with empty strings... so we have to fill them in with spaces.. 
    private String replaceEmptyStrings(String data) {
        if (data != null) return data.replace("\"\"", "\" \"");
        return null;
    }
    
    // parse the JSON...
    protected Object parseJson(String json) {
        String modified_json = this.replaceEmptyStrings(json);
        Object parsed = null;
        try {
            if (json != null && json.contains("{") && json.contains("}"))
                parsed = this.jsonParser().parseJson(modified_json);
        }
        catch (JSONParsingException | NullPointerException ex) {
            this.manager().errorLogger().warning("JSON parsing exception for: " + modified_json + " message: " + ex.getMessage(),ex);
            parsed = null;
        }
        return parsed;
    }
    
    // protected getters/setters...
    protected JSONParser jsonParser() { return this.m_json_parser; }
    protected JSONGenerator jsonGenerator() { return this.m_json_generator; }
    protected Orchestrator manager() { return this.m_manager; }
    protected void setDomain(String mds_domain) { this.m_mds_domain = mds_domain; }
    protected String getDomain() { return this.getDomain(false); }
    protected String getDomain(boolean show_default) {
        if (this.m_mds_domain != null) return "/" + this.m_mds_domain; 
        if (show_default == true) return "/" + this.m_def_domain;
        return "";
    }
}
