/*
 * Copyright 2014 Ted Dunning
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

package org.apache.mahout.pig;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.classifier.sgd.PolymorphicWritable;
import org.apache.pig.impl.util.UDFContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

/**
 * Writable wrapper for a classifier and a list of target categories.
 */
public class Classifier implements Writable {
    private List<String> categories;
    private OnlineLogisticRegression model;

    public Classifier() {
        categories = Lists.newArrayList();
    }

    public Classifier(List<String> categories, OnlineLogisticRegression model) {
        this.categories = Lists.newArrayList(categories);
        this.model = model;
    }

    public Classifier(String file) {
        try {
            Configuration conf = UDFContext.getUDFContext().getJobConf();
            final FSDataInputStream in = FileSystem.get(conf).open(new Path(file));
            try {
                this.readFields(in);
            } finally {
                in.close();
            }
        } catch (FileNotFoundException e) {
            throw new BadClassifierSpecException("Can't find file to read model from: " + file, e);
        } catch (IOException e) {
            throw new BadClassifierSpecException("Error while reading model from: " + file, e);
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(categories.size());
        for (String category : categories) {
            new Text(category).write(dataOutput);
        }
        PolymorphicWritable.write(dataOutput, model);
    }

    public void readFields(DataInput dataInput) throws IOException {
        int n = dataInput.readInt();
        for (int i = 0; i < n; i++) {
            Text tmp = new Text();
            tmp.readFields(dataInput);
            categories.add(tmp.toString());
        }
        model = PolymorphicWritable.read(dataInput, OnlineLogisticRegression.class);
    }

    public List<String> getCategories() {
        return categories;
    }

    public OnlineLogisticRegression getModel() {
        return model;
    }

}
