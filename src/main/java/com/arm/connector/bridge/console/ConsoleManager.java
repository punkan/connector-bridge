/**
 * @file    ConsoleManager.java
 * @brief   console manager for the connector bridge (unused)
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

package com.arm.connector.bridge.console;

import com.arm.connector.bridge.coordinator.Orchestrator;
import com.arm.connector.bridge.core.BaseClass;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author danson
 */
public class ConsoleManager extends BaseClass {
    private Orchestrator m_orchestrator = null;
    
    public ConsoleManager(Orchestrator orchestrator) {
        super(orchestrator.errorLogger(),orchestrator.preferences());
        this.m_orchestrator = orchestrator;
    }
    
    // process the console request for the connector bridge
    @SuppressWarnings("empty-statement")
    public void processConsole(HttpServletRequest request, HttpServletResponse response) {
        // not implemented 
        ;
    }
}
