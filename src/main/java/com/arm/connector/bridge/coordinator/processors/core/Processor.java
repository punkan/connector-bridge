/**
 * @file    Processor.java
 * @brief   peer processor base class
 * @author  Doug Anson
 * @version 1.0
 * @see
 *
 * Copyright (c) 2016 ARM
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.arm.connector.bridge.coordinator.processors.core;

import com.arm.connector.bridge.coordinator.Orchestrator;
import com.arm.connector.bridge.core.BaseClass;
import com.codesnippets4all.json.exceptions.JSONParsingException;
import com.codesnippets4all.json.generators.JSONGenerator;
import com.codesnippets4all.json.parsers.JSONParser;

/**
 * Peer Processor base class
 * @author Doug Anson
 */
public class Processor extends BaseClass {
    private Orchestrator     m_orchestrator = null;
    private JSONGenerator    m_json_generator = null;
    private JSONParser       m_json_parser = null;
    protected String         m_mds_domain = null;
    private String           m_def_domain = null;
    
    public Processor(Orchestrator orchestrator,String suffix) {
        super(orchestrator.errorLogger(),orchestrator.preferences());
        this.m_orchestrator = orchestrator;
        this.m_def_domain = orchestrator.preferences().valueOf("mds_def_domain",suffix);
        this.m_json_parser = orchestrator.getJSONParser();
        this.m_json_generator = orchestrator.getJSONGenerator();
    }
    
    // create the authentication hash
    protected String createAuthenticationHash() {
        return this.orchestrator().createAuthenticationHash();
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
            this.orchestrator().errorLogger().warning("JSON parsing exception for: " + modified_json + " message: " + ex.getMessage(),ex);
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
