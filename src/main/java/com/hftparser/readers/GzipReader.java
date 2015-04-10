package com.hftparser.readers;

import com.hftparser.containers.WaitFreeQueue;
import org.apache.commons.lang.mutable.MutableBoolean;

import java.io.*;
import java.util.zip.GZIPInputStream;

public class GzipReader implements Runnable {
    // InputStream file;
    private BufferedReader reader;
    private final WaitFreeQueue<String> queue;
    private final MutableBoolean pipelineError;

    // TODO: find out what encoding the files actually use, and set it
    // correctly for InputStreamReader

    // http://stackoverflow.com/questions/1080381/
    public GzipReader(InputStream file, WaitFreeQueue<String> _queue, MutableBoolean pipelineError) throws IOException {
        this.pipelineError = pipelineError;
        queue = _queue;

        InputStream gzipStream = new GZIPInputStream(file);
        Reader decoder = new InputStreamReader(gzipStream);
        reader = new BufferedReader(decoder);
    }

    public GzipReader(InputStream file, WaitFreeQueue<String> _queue, BufferedReader reader) throws IOException {
        this(file, _queue, new MutableBoolean());
    }

    public void run() {
        try {
            String toEnq;
            while (!pipelineError.booleanValue() && ((toEnq = reader.readLine()) != null)) {
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
