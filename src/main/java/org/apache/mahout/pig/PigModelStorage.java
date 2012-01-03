package org.apache.mahout.pig;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.classifier.sgd.PolymorphicWritable;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Stores models in files named by a well known prefix and the reduce key.
 */
public class PigModelStorage extends StoreFunc {
    private RecordWriter output;

    public PigModelStorage() {
    }


    /**
     * Set the schema for data to be stored.  This will be called on the
     * front end during planning if the store is associated with a schema.
     * A Store function should implement this function to
     * check that a given schema is acceptable to it.  For example, it
     * can check that the correct partition keys are included;
     * a storage function to be written directly to an OutputFormat can
     * make sure the schema will translate in a well defined way.  Default implementation
     * is a no-op.
     *
     * @param s to be checked
     * @throws java.io.IOException if this schema is not acceptable.  It should include
     *                             a detailed error message indicating what is wrong with the schema.
     */
    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        System.out.printf("Into checkSchema with keys: %s\n%s\n", Arrays.deepToString(s.fieldNames()), s);
        if (s.getFields()[1].getType() != DataType.BYTEARRAY || s.getFields()[0].getType() == DataType.BYTEARRAY) {
            throw new InvalidOutputSchema(String.format("Want a key with a string format and binary model for model output but got %s and %s",
                    DataType.findTypeName(s.getFields()[0].getType()), DataType.findTypeName(s.getFields()[1].getType())));
        }
    }


    /**
     * Return the OutputFormat associated with StoreFunc.  This will be called
     * on the front end during planning and on the backend during
     * execution.
     *
     * @return the {@link org.apache.hadoop.mapreduce.OutputFormat} associated with StoreFunc
     * @throws java.io.IOException if an exception occurs while constructing the
     *                             OutputFormat
     */
    @Override
    public OutputFormat getOutputFormat() throws IOException {
        return new SequenceFileOutputFormat();
    }

    /**
     * Communicate to the storer the location where the data needs to be stored.
     * The location string passed to the {@link org.apache.pig.StoreFunc} here is the
     * return value of {@link org.apache.pig.StoreFunc#relToAbsPathForStoreLocation(String, org.apache.hadoop.fs.Path)}
     * This method will be called in the frontend and backend multiple times. Implementations
     * should bear in mind that this method is called multiple times and should
     * ensure there are no inconsistent side effects due to the multiple calls.
     * {@link #checkSchema(org.apache.pig.ResourceSchema)} will be called before any call to
     * {@link #setStoreLocation(String, org.apache.hadoop.mapreduce.Job)}.
     *
     * @param location Location returned by
     *                 {@link org.apache.pig.StoreFunc#relToAbsPathForStoreLocation(String, org.apache.hadoop.fs.Path)}
     * @param job      The {@link org.apache.hadoop.mapreduce.Job} object
     * @throws java.io.IOException if the location is not valid.
     */
    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        FileOutputFormat.setOutputPath(job, new Path(location));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Classifier.class);
    }

    /**
     * Initialize StoreFunc to write data.  This will be called during
     * execution on the backend before the call to putNext.
     *
     * @param writer RecordWriter to use.
     * @throws java.io.IOException if an exception occurs during initialization
     */
    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
        this.output = writer;
    }

    /**
     * Write a tuple to the data store.
     *
     * @param t the tuple to store.
     * @throws java.io.IOException if an exception occurs during the write
     */
    @Override
    public void putNext(Tuple t) throws IOException {
        try {
            System.out.printf("value key = %s\n", t.get(0));
            Classifier r;
            try {
                DataInputStream in = new DataInputStream(new ByteArrayInputStream(((DataByteArray) t.get(1)).get()));
                r = PolymorphicWritable.read(in, Classifier.class);
                in.close();
            } catch (IOException e) {
                throw new ImpossibleStateError("Can't have error in BAIS", e);
            }

            output.write(new Text(DataType.toString(t.get(0))), r);
        } catch (InterruptedException e) {
            throw new ImpossibleStateError("Interrupted operation ... don't know what to do", e);
        }
    }
}
