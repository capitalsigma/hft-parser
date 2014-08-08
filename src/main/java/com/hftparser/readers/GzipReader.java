package com.hftparser.readers;

import com.hftparser.containers.WaitFreeQueue;

import java.io.*;
import java.util.zip.GZIPInputStream;

public class GzipReader implements Runnable {
    // InputStream file;
    private BufferedReader reader;
    private final WaitFreeQueue<String> queue;

    // TODO: find out what encoding the files actually use, and set it
    // correctly for InputStreamReader

    // http://stackoverflow.com/questions/1080381/
    public GzipReader(InputStream file, WaitFreeQueue<String> _queue) throws IOException {
        queue = _queue;

        InputStream gzipStream = new GZIPInputStream(file);
        Reader decoder = new InputStreamReader(gzipStream);
        reader = new BufferedReader(decoder);
    }

    public void run() {
        try {
            String toEnq;
            while ((toEnq = reader.readLine()) != null) {
                //                System.out.println("Reader read a line:" + toEnq);
                // loop until we successfully enqueue our new line
                //noinspection StatementWithEmptyBody
                while (!queue.enq(toEnq)) {
                }
            }

            reader.close();
        } catch (IOException exn) {
            System.err.println("Got error: " + exn.getMessage());
        } finally {
            // stop everyone else working
            queue.acceptingOrders = false;
        }
    }
}
