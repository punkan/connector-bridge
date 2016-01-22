/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.arm.connector.bridge.console;

import com.arm.connector.bridge.coordinator.Orchestrator;
import com.arm.connector.bridge.coordinator.processors.interfaces.MDSInterface;
import com.arm.connector.bridge.coordinator.processors.arm.MDSProcessor;
import com.arm.connector.bridge.core.BaseClass;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author danson
 */
public class ConsoleManager extends BaseClass {
    private Orchestrator m_manager = null;
    
    public ConsoleManager(Orchestrator manager) {
        super(manager.errorLogger(),manager.preferences());
        this.m_manager = manager;
    }
    
    // process the console request for the connector bridge
    @SuppressWarnings("empty-statement")
    public void processConsole(HttpServletRequest request, HttpServletResponse response) {
        // not implemented 
        ;
    }
}
