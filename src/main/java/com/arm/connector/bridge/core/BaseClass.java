/**
 * @file    BaseClass.java
 * @brief   base class for connector bridge
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
