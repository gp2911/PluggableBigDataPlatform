package com.ganesh.bigdata.platform.core.util;

import java.io.*;

/**
 * Utility class to take care of redirecting childProcess's output stream to stdout/file.
 * @author ganesh
 */
public class StreamGobbler extends Thread {
    InputStream is;
    PrintStream ps;
    // reads everything from is until empty.
    public StreamGobbler(InputStream is) {
        this.is = is;
        this.ps = System.out;
    }

    public StreamGobbler(InputStream is, PrintStream ps){
        this.is = is;
        this.ps = ps;
    }

    public void run() {

        try {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line=null;
            while ( (line = br.readLine()) != null)
                ps.println(line);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
}