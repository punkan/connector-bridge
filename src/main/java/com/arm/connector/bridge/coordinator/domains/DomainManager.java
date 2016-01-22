/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.arm.connector.bridge.coordinator.domains;

import com.arm.connector.bridge.coordinator.Orchestrator;
import com.arm.connector.bridge.core.ErrorLogger;
import com.arm.connector.bridge.preferences.PreferenceManager;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author Doug Anson
 */
public class DomainManager {
    private String              m_domain = null;
    private Orchestrator    m_endpoint_manager = null;
    private ErrorLogger         m_error_logger = null;
    private PreferenceManager   m_preference_manager = null;
    
    // constructor
    public DomainManager(ErrorLogger error_logger,PreferenceManager preference_manager,String domain) {
        this.m_domain = domain;
        this.m_error_logger = error_logger;
        this.m_preference_manager = preference_manager;
        this.m_endpoint_manager = new Orchestrator(error_logger,preference_manager,domain);
    }
    
    public void processConsole(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException  {
        this.m_endpoint_manager.processConsole(request, response);
    }
    
    public void processNotification(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException  {
        this.m_endpoint_manager.processNotification(request, response);
    }
    
    // get the endpoints manager
    public Orchestrator getEndpointsManager() { return this.m_endpoint_manager; }
    
    // get the domain name...
    public String domain() { return this.m_domain; } 
}
