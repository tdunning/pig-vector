package org.apache.mahout.pig;

import com.google.common.collect.Lists;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Wraps a pig tuple around a Mahout vector.
 */
public class PigVector implements Tuple {
    private Vector v = null;

    public PigVector(Vector v) {
        this.v = v;
    }

    public Vector getV() {
        return v;
    }

    /**
     * Make this tuple reference the contents of another.  This method does not copy the underlying
     * data.   It maintains references to the data from the original tuple (and possibly even to the
     * data structure holding the data).
     *
     * @param t Tuple to reference.
     */
    public void reference(Tuple t) {
        throw new UnsupportedOperationException("Default operation");
    }

    /**
     * Find the size of the tuple.  Used to be called arity().
     *
     * @return number of fields in the tuple.
     */
    public int size() {
        return v.size();
    }

    /**
     * Find out if a given field is null.
     *
     * @param fieldNum Number of field to check for null.
     * @return true if the field is null, false otherwise.
     * @throws org.apache.pig.backend.executionengine.ExecException
     *          if the field number given is greater than or equal to the number of fields in the
     *          tuple.
     */
    public boolean isNull(int fieldNum) throws ExecException {
        return fieldNum < 0 || fieldNum >= v.size();
    }

    /**
     * Find the type of a given field.
     *
     * @param fieldNum Number of field to get the type for.
     * @return type, encoded as a byte value.  The values are defined in {@link
     *         org.apache.pig.data.DataType}.  If the field is null, then DataType.UNKNOWN will be
     *         returned.
     * @throws org.apache.pig.backend.executionengine.ExecException
     *          if the field number is greater than or equal to the number of fields in the tuple.
     */
    public byte getType(int fieldNum) throws ExecException {
        return DataType.DOUBLE;
    }

    /**
     * Get the value in a given field.
     *
     * @param fieldNum Number of the field to get the value for.
     * @return value, as an Object.
     * @throws org.apache.pig.backend.executionengine.ExecException
     *          if the field number is greater than or equal to the number of fields in the tuple.
     */
    public Object get(int fieldNum) throws ExecException {
        return v.get(fieldNum);
    }

    /**
     * Get all of the fields in the tuple as a list.
     *
     * @return a list of objects containing the fields of the tuple in order.
     */
    public List<Object> getAll() {
        List<Object> r = Lists.newArrayList();
        for (Vector.Element element : v) {
            r.add(element.get());
        }
        return r;
    }

    /**
     * Set the value in a given field.  This should not be called unless the tuple was constructed by
     * {@link org.apache.pig.data.TupleFactory#newTuple(int)} with an argument greater than the
     * fieldNum being passed here.  This call will not automatically expand the tuple size.  That is if
     * you called {@link org.apache.pig.data.TupleFactory#newTuple(int)} with a 2, it is okay to call
     * this function with a 1, but not with a 2 or greater.
     *
     * @param fieldNum Number of the field to set the value for.
     * @param val      Object to put in the indicated field.
     * @throws org.apache.pig.backend.executionengine.ExecException
     *          if the field number is greater than or equal to the number of fields in the tuple.
     */
    public void set(int fieldNum, Object val) throws ExecException {
        v.set(fieldNum, (Double) val);
    }

    /**
     * Append a field to a tuple.  This method is not efficient as it may force copying of existing
     * data in order to grow the data structure. Whenever possible you should construct your Tuple with
     * {@link org.apache.pig.data.TupleFactory#newTuple(int)} and then fill in the values with {@link
     * #set(int, Object)}, rather than construct it with {@link org.apache.pig.data.TupleFactory#newTuple()}
     * and append values.
     *
     * @param val Object to append to the tuple.
     */
    public void append(Object val) {
        throw new UnsupportedOperationException("Default operation");
    }

    /**
     * Determine the size of tuple in memory.  This is used by data bags to determine their memory
     * size.  This need not be exact, but it should be a decent estimation.
     *
     * @return estimated memory size, in bytes.
     */
    public long getMemorySize() {
        return v.size() * 8;
    }

    /**
     * Write a tuple of atomic values into a string.  All values in the tuple must be atomic (no bags,
     * tuples, or maps).
     *
     * @param delim Delimiter to use in the string.
     * @return A string containing the tuple.
     * @throws org.apache.pig.backend.executionengine.ExecException
     *          if a non-atomic value is found.
     */
    public String toDelimitedString(String delim) throws ExecException {
        return v.asFormatString();
    }

    /**
     * Determine if this entire tuple (not any particular field) is null.
     *
     * @return true if this Tuple is null
     */
    public boolean isNull() {
        return v == null;
    }

    /**
     * Mark this entire tuple as null or not null.
     *
     * @param isNull boolean indicating whether this tuple is null
     */
    public void setNull(boolean isNull) {
        throw new UnsupportedOperationException("Default operation");
    }

    public int compareTo(Object o) {
        throw new UnsupportedOperationException("Default operation");
    }

    public void write(DataOutput dataOutput) throws IOException {
        new VectorWritable(v).write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        VectorWritable vw = new VectorWritable();
        vw.readFields(dataInput);
        v = vw.get();
    }
}
