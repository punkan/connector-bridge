/* Copyright (C) 2013 ARM
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software
 * and associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
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
