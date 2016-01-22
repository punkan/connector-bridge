/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.arm.connector.bridge.coordinator.domains;

import com.arm.connector.bridge.core.ErrorLogger;
import com.arm.connector.bridge.preferences.PreferenceManager;
import com.arm.connector.bridge.transport.HttpTransport;
import java.util.ArrayList;

/**
 *
 * @author Doug Anson
 */
public class DomainChecker {
    private HttpTransport           m_http = null;
    private ErrorLogger             m_error_logger = null;
    private PreferenceManager       m_preference_manager = null;
    
    public DomainChecker(ErrorLogger error_logger,PreferenceManager preference_manager) {
        this.m_error_logger = error_logger;
        this.m_preference_manager = preference_manager;
        this.m_http = new HttpTransport(this.m_error_logger,this.m_preference_manager);
    }
    
    // get the domain list
    public ArrayList<String> getDomainList() {
        String url = this.createDomainURL();
        
        // DEBUG
        //this.m_error_logger.info("getDomainList: URL: " + url);
        
        // dispatch and get a response...
        String response = this.m_http.httpsGet(url,this.m_preference_manager.valueOf("mds_admin_username"),
                                                   this.m_preference_manager.valueOf("mds_admin_password"),null,null,null);
        
        // return the parsed response
        return this.parseDomainList(response);
    }
    
    // create the URL to get the current domain list from mDS
    private String createDomainURL() {
        return "https://" + this.m_preference_manager.valueOf("mds_address") + ":" 
                          + this.m_preference_manager.valueOf("mds_admin_port") + "/" 
                          + this.m_preference_manager.valueOf("mds_domain_uri");
    }
    
    // parse the received domain list
    @SuppressWarnings("empty-statement")
    private ArrayList<String> parseDomainList(String list) {
        ArrayList<String> domains = new ArrayList<>();
        
        // quick parse
        try {
            String[] array_list = list.replace("[","").replace("]", "").replace("\"","").replace(",", " ").split(" ");
            for(int i=0;i<array_list.length;++i) {
                domains.add(array_list[i]);
            }
        }
        catch (Exception ex) {
            // fail silently...
            ;
        }
        
        // DEBUG
        //this.m_error_logger.info("Parsed Domains: " + domains);
        
        // return the list of domains received
        return domains;
    }
}
