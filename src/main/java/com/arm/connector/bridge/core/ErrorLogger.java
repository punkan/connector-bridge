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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;

/**
 * Error Handler
 *
 * @author Doug Anson
 */
public class ErrorLogger extends BaseClass {

    // Default message
    /**
     *
     */
    public static String DEFAULT_MESSAGE = "<No Message-OK>";

    // Logging classifications
    /**
     *
     */
    public static int INFO = 0x0001;     // informational

    /**
     *
     */
    public static int WARNING = 0x0002;     // warning

    /**
     *
     */
    public static int CRITICAL = 0x0004;     // critical error

    // masks
    /**
     *
     */
    public static int SHOW_ALL = 0x00FF;     // show all

    /**
     *
     */
    public static int SHOW_INFO = 0x0001;     // show INFO only

    /**
     *
     */
    public static int SHOW_WARNING = 0x0002;     // show WARNING only

    /**
     *
     */
    public static int SHOW_CRITICAL = 0x0004;     // show CRITICAL only

    // maxmium number of tracked log entries
    /**
     *
     */
    public static int MAX_LOG_ENTRIES = 9000;     // reset the list after retaining this many entries

    private String m_message;              // our message
    private Exception m_exception;            // our exception
    private int m_level;                // error classification level
    private int m_mask = SHOW_ALL;      // default error classification mask
    private volatile ArrayList<String> m_log = null;           // error log

    // constructor
    /**
     *
     */
    public ErrorLogger() {
        super(null, null);
        this.m_message = ErrorLogger.DEFAULT_MESSAGE;
        this.m_exception = null;
        this.m_level = ErrorLogger.INFO;
        this.m_mask = SHOW_ALL;
        this.m_log = new ArrayList<>();
    }

    // set the mask
    /**
     *
     * @param mask
     */
    public void setLoggingMask(int mask) {
        this.m_mask = mask;
    }

    // buffer the log entry
    private void buffer(String entry) {
        if (entry != null && entry.length() > 0) {
            if (this.m_log.size() >= MAX_LOG_ENTRIES) {
                this.m_log.clear();
            }
            this.m_log.add(entry);
        }
    }

    // log entry - messages only
    /**
     *
     * @param message
     */
    public void info(String message) {
        this.log(ErrorLogger.INFO, message, null);
    }

    /**
     *
     * @param message
     */
    public void warning(String message) {
        this.log(ErrorLogger.WARNING, message, null);
    }

    /**
     *
     * @param message
     */
    public void critical(String message) {
        this.log(ErrorLogger.CRITICAL, message, null);
    }

    // log entry - messages and exceptions
    /**
     *
     * @param message
     * @param ex
     */
    public void info(String message, Exception ex) {
        this.log(ErrorLogger.INFO, message, ex);
    }

    /**
     *
     * @param message
     * @param ex
     */
    public void warning(String message, Exception ex) {
        this.log(ErrorLogger.WARNING, message, ex);
    }

    /**
     *
     * @param message
     * @param ex
     */
    public void critical(String message, Exception ex) {
        this.log(ErrorLogger.CRITICAL, message, ex);
    }

    // log entry - exceptions only
    /**
     *
     * @param ex
     */
    public void info(Exception ex) {
        this.log(ErrorLogger.INFO, ErrorLogger.DEFAULT_MESSAGE, ex);
    }

    /**
     *
     * @param ex
     */
    public void warning(Exception ex) {
        this.log(ErrorLogger.WARNING, ErrorLogger.DEFAULT_MESSAGE, ex);
    }

    /**
     *
     * @param ex
     */
    public void critical(Exception ex) {
        this.log(ErrorLogger.CRITICAL, ErrorLogger.DEFAULT_MESSAGE, ex);
    }

    // log a message (base)
    private void log(int level, String message, Exception exception) {
        this.m_message = message;
        this.m_exception = exception;
        this.m_level = level;
        this.log();
    }

    // post the log
    private void log() {
        // check what level we want to display
        if ((this.m_mask & this.m_level) != 0) {
            if (this.m_exception != null) {
                if (this.m_message != null) {
                    // log the message
                    System.out.println(this.m_level + ": " + this.m_message + " Exception: " + this.m_exception + ". StackTrace: " + this.stackTraceToString(this.m_exception));
                    this.buffer(this.m_message + " Exception: " + this.m_exception + ". StackTrace: " + this.stackTraceToString(this.m_exception));                   
                 }
                else {
                    // log the exception
                    System.out.println(this.m_level + ": " + this.m_exception);
                    this.buffer("" + this.m_exception);
                    System.out.println(this.m_level + ": " + this.stackTraceToString(this.m_exception));
                }
            }
            // log what we have
            else {
                if (this.m_message != null) {
                    // log the message
                    System.out.println(this.m_level + ": " + this.m_message);
                    this.buffer(this.m_message);
                }
                else {
                    // no message
                    this.m_message = "UNKNOWN ERROR";
                    
                    // log the message
                    System.out.println(this.m_level + ": " + this.m_message);
                    this.buffer(this.m_message);
                }
            }
        }
    }

    // convert a stack trace to a string
    private String stackTraceToString(Exception ex) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        ex.printStackTrace(pw);
        return sw.toString();
    }
}
