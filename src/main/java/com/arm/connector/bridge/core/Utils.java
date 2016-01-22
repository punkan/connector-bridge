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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

/**
 * Static support utilities
 * @author Doug Anson
 */
public class Utils {
    // static variables
    private static char[] hexArray = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    private static String __cache_hash = null;
    private static String _externalIPAddress = null;

    // get local timezone offset from UTC in milliseconds
    public static int getUTCOffset() {
        TimeZone tz = TimeZone.getDefault();
        Calendar cal = GregorianCalendar.getInstance(tz);
        return tz.getOffset(cal.getTimeInMillis());
    }

    // get the local time in seconds since Jan 1 1970
    public static int getLocalTime() {
        int utc = (int) (System.currentTimeMillis() / 1000);
        int localtime = utc;

        return localtime;
    }

    // get UTC time in seconds since Jan 1 1970
    public static long getUTCTime() {
        return Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTimeInMillis() - Utils.getUTCOffset();
    }

    // get our base URL
    public static String getBaseURL(String endpoint, HttpServletRequest request) {
        String url = "";
        try {
            url = request.getRequestURL().toString().replace(request.getRequestURI().substring(1), request.getContextPath());
            url += "//" + endpoint;
            url = url.replace("://", "_TEMP_");
            url = url.replace("//", "/");
            url = url.replace("_TEMP_", "://");
        }
        catch (Exception ex) {
            url = request.getRequestURL().toString();
        }
        return url;
    }

    // convert boolean to string
    public static String booleanToString(boolean val) {
        if (val) {
            return "true";
        }
        return "false";
    }

    // convert string to boolean
    public static boolean stringToBoolean(String val) {
        boolean bval = false;
        if (val != null && val.equalsIgnoreCase("true")) {
            bval = true;
        }
        return bval;
    }

    // START DATE FUNCTIONS
    
    // get the current date and time
    public static java.util.Date now() {
        java.util.Date rightnow = new java.util.Date(System.currentTimeMillis());
        return rightnow;
    }

    // convert a JAVA Date to a SQL Timestamp and back
    public static java.sql.Timestamp convertDate(java.util.Date date) {
        java.sql.Timestamp sql_date = new java.sql.Timestamp(date.getTime());
        sql_date.setTime(date.getTime());
        return sql_date;
    }

    // convert SQL Date to Java Date
    public static java.util.Date convertDate(java.sql.Timestamp date) {
        java.util.Date java_date = new java.util.Date(date.getTime());
        return java_date;
    }

    // convert a Date to a String (java)
    public static String dateToString(java.util.Date date) {
        return Utils.dateToString(date, "MM/dd/yyyy HH:mm:ss");
    }

    // Date to Date String
    public static String dateToString(java.util.Date date, String format) {
        if (date != null) {
            DateFormat df = new SimpleDateFormat(format);
            return df.format(date);
        }
        else {
            return "[no date]";
        }
    }

    // convert a SQL Timestamp to a String (SQL)
    public static String dateToString(java.sql.Timestamp timestamp) {
        if (timestamp != null) {
            return Utils.dateToString(new java.util.Date(timestamp.getTime()));
        }
        else {
            return "[no date]";
        }
    }

    // convert a Date to a String (SQL)
    public static String dateToString(java.sql.Date date) {
        if (date != null) {
            return Utils.dateToString(new java.util.Date(date.getTime()));
        }
        else {
            return "[no date]";
        }
    }

    // convert a String (Java) to a java.util.Date object
    public static java.util.Date stringToDate(ErrorLogger err, String str_date) {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
            String stripped = str_date.replace('"', ' ').trim();
            return dateFormat.parse(stripped);
        }
        catch (ParseException ex) {
            err.warning("Unable to parse string date: " + str_date + " to format: \"MM/dd/yyyy HH:mm:ss\"", ex);
        }
        return null;
    }

    // END DATE FUNCTIONS
    
    // Hex String to ByteBuffer or byte[]
    public static ByteBuffer hexStringToByteBuffer(String str) {
        return ByteBuffer.wrap(Utils.hexStringToByteArray(str));
    }

    // hex String to ByteArray
    public static byte[] hexStringToByteArray(String str) {
        int len = str.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(str.charAt(i), 16) << 4) + Character.digit(str.charAt(i + 1), 16));
        }
        return data;
    }

    // convert a hex byte array to a string
    public static String bytesToHexString(ByteBuffer bytes) {
        return Utils.bytesToHexString(bytes.array());
    }

    // ByteArray to hex string
    public static String bytesToHexString(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; ++j) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    // read in a HTML file
    public static String readHTMLFileIntoString(HttpServlet svc, ErrorLogger err, String filename) {
        try {
            String text = null;
            String file = "";
            ServletContext context = svc.getServletContext();
            try (InputStream is = context.getResourceAsStream("/" + filename); InputStreamReader isr = new InputStreamReader(is); BufferedReader reader = new BufferedReader(isr)) {
                while ((text = reader.readLine()) != null) {
                    file += text;
                }
            }
            return file;
        }
        catch (IOException ex) {
            err.critical("error while trying to read HTML template: " + filename, ex);
        }
        return null;
    }
    
    // decode CoAP payload Base64
    public static String decodeCoAPPayload(String payload) {
        String decoded = null;
        
        try {
            String b64_payload = payload.replace("\\u003d", "=");
            Base64 decoder = new Base64();
            byte[] data = decoder.decode(b64_payload);
            decoded = new String(data);
        }
        catch (Exception ex) {
            decoded = "<unk>";
        }
        
        return decoded;
    }
    
    // create a URL-safe Token
    public static String createURLSafeToken(String seed) {
        byte[] b64 = Base64.encodeBase64(seed.getBytes());
        return new String(b64);
    }
    
    // create Authentication Hash
    public static String createHash(String data) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(data.getBytes());
            String hex = Hex.encodeHexString(digest);
            return Base64.encodeBase64URLSafeString(hex.getBytes());
        }
        catch (NoSuchAlgorithmException ex) {
            return "none";
        }
    }
    
    // validate the Authentication Hash
    public static boolean validateHash(String header_hash,String calc_hash) {
        boolean validated = false;
        try {
            if (Utils.__cache_hash == null) {
                validated = (header_hash != null && calc_hash != null && calc_hash.equalsIgnoreCase(header_hash) == true);
                if (validated && Utils.__cache_hash == null) {
                    Utils.__cache_hash = header_hash;
                }
            }
            else {
                validated = (header_hash != null && Utils.__cache_hash != null && Utils.__cache_hash.equalsIgnoreCase(header_hash) == true);
            }
            return validated;
        }
        catch (Exception ex) {
            return false;
        }
    }
    
    // get our external IP Address
    public static String getExternalIPAddress() {
        if (Utils._externalIPAddress == null) {
            BufferedReader in = null;
            try {
                URL whatismyip = new URL("http://checkip.amazonaws.com");
                in = new BufferedReader(new InputStreamReader(whatismyip.openStream()));
                Utils._externalIPAddress = in.readLine();
                in.close();
            }
            catch (Exception ex) {
                try {
                    if (in != null) in.close();
                }
                catch(Exception ex2) {
                    // silent
                }
            }
        }
        return Utils._externalIPAddress;
    }
}
