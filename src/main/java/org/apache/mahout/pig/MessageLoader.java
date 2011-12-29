package org.apache.mahout.pig;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.List;

/**
 * Reads messages stored one per file such as in the 20 newsgroups data.
 */
public class MessageLoader extends LoadFunc {
    private RecordReader<LongWritable, Text> reader;
    private String location;

    @Override
    public void setLocation(String location, Job job) throws IOException {
        this.location = location;
        FileInputFormat.setInputPaths(job, this.location);
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new TextInputFormat() {

            @Override
            protected boolean isSplitable(JobContext context, Path file) {
                return false;
            }

            @Override
            public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
                location = ((FileSplit) split).getPath().toString();
                return super.createRecordReader(split, context);    //To change body of overridden methods use File | Settings | File Templates.
            }
        };
    }

    /**
     * Stores our reader so we can get bytes.
     */
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        this.reader = reader;
    }

    /**
     * Retrieves a message.  The resulting tuple has the directory name, the file name,
     * the subject line and the text.
     */
    @Override
    public Tuple getNext() throws IOException {
        try {
            String subject = "";
            while (reader.nextKeyValue()) {
                final Text line = reader.getCurrentValue();
                if (line.find("Subject: ") == 0) {
                    subject = line.toString().replace("Subject: ", "");
                }
                if (line.getLength() == 0) {
                    break;
                }
            }

            List<String> contents = Lists.newArrayList();
            while (reader.nextKeyValue()) {
                final String line = reader.getCurrentValue().toString();
                if (line.equals("--")) {
                    break;
                }
                if (!line.startsWith(">")) {
                    contents.add(line);
                }
            }
            if (subject.length() == 0 && contents.size() == 0) {
                return null;
            }
            Tuple r = TupleFactory.getInstance().newTuple();

            // directory without leading path
            r.append(location.replaceAll("/[^/]*$", "").replaceAll(".*/", ""));
            // file name without any directory
            r.append(location.replaceAll(".*/", ""));
            // subject line
            r.append(subject);
            // all the rest of the text
            r.append(Joiner.on(" ").join(contents));

            return r;
        } catch (InterruptedException e) {
            throw new ImpossibleStateError("Interrupted!");
        }
    }
}
