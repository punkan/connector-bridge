/**
 * @file    BaseClass.java
 * @brief   base class for connector bridge
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

package com.arm.connector.bridge.core;

import com.arm.connector.bridge.preferences.PreferenceManager;

/**
 * Base Class for fundamental logging and preferenceManager support
 *
 * @author Doug Anson
 */
public class BaseClass {

    private ErrorLogger m_error_logger = null;
    private PreferenceManager m_preference_manager = null;

    // constructor
    /**
     *
     * @param error_logger
     * @param preference_manager
     */
    public BaseClass(ErrorLogger error_logger, PreferenceManager preference_manager) {
        this.m_error_logger = error_logger;
        this.m_preference_manager = preference_manager;
    }

    // get our error handler
    /**
     *
     * @return
     */
    public com.arm.connector.bridge.core.ErrorLogger errorLogger() {
        return this.m_error_logger;
    }

    // get the preferenceManager
    /**
     *
     * @return
     */
    public com.arm.connector.bridge.preferences.PreferenceManager preferences() {
        return this.m_preference_manager;
    }

    /**
     *
     * @param key
     * @return
     */
    protected String prefValue(String key) {
        return this.prefValue(key,null);
    }
    
    /**
     *
     * @param key
     * @param suffix
     * @return
     */
    protected String prefValue(String key,String suffix) {
        if (this.m_preference_manager != null) {
            return this.m_preference_manager.valueOf(key,suffix);
        }
        return null;
    }
    
    /**
     *
     * @param key
     * @param def_value
     * @return
     */
    protected String prefValueWithDefault(String key,String def_value) {
        return this.prefValueWithDefault(key, null, def_value);
    }
    
    /**
     *
     * @param key
     * @param suffix
     * @param def_value
     * @return
     */
    protected String prefValueWithDefault(String key,String suffix,String def_value) {
        String value = this.prefValue(key,suffix);
        if (value != null && value.length() > 0) {
            return value;
        }
        return def_value;
    }
    
    /**
     *
     * @param key
     * @return
     */
    protected int prefIntValue(String key) {
        return this.prefIntValue(key,null);
    }
    
    /**
     *
     * @param key
     * @param suffix
     * @return
     */
    protected int prefIntValue(String key,String suffix) {
        if (this.m_preference_manager != null) {
            return this.m_preference_manager.intValueOf(key,suffix);
        }
        return -1;
    }
    
    /**
     *
     * @param key
     * @return
     */
    protected float prefFloatValue(String key) {
        return this.prefFloatValue(key,null);
    }
    
    /**
     *
     * @param key
     * @param suffix
     * @return
     */
    protected float prefFloatValue(String key,String suffix) {
        if (this.m_preference_manager != null) {
            return this.m_preference_manager.floatValueOf(key,suffix);
        }
        return (float)-1.0;
    }

    /**
     *
     * @param key
     * @return
     */
    protected boolean prefBoolValue(String key) {
        return this.prefBoolValue(key,null);
    }
    
    /**
     *
     * @param key
     * @param suffix
     * @return
     */
    protected boolean prefBoolValue(String key,String suffix) {
        if (this.m_preference_manager != null) {
            return this.m_preference_manager.booleanValueOf(key,suffix);
        }
        return false;
    }
}
