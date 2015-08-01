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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.mahout.classifier.sgd.PolymorphicWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;
import org.apache.mahout.math.Vector;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Evaluates a logistic regression classifier
 */
public class LogisticRegressionEval extends EvalFunc<String> {
    private Classifier model;
    private String location;
    private String key;

    public LogisticRegressionEval(String modelParams) throws IOException {
        System.out.printf("Model params = %s\n", modelParams);

        Splitter onComma = Splitter.on(",").trimResults().omitEmptyStrings();
        Splitter onEquals = Splitter.on("=").trimResults();
        Joiner withSpaces = Joiner.on(" ");

        Map<String, String> options = Maps.newHashMap();

        for (String option : onComma.split(modelParams)) {
            List<String> values = Lists.newArrayList(onEquals.split(option));
            options.put(values.get(0), values.get(1));
        }

        if (options.containsKey("sequence")) {
            location = options.get("sequence");
            if (options.containsKey("key")) {
                key = options.get("key");
                options.remove("sequence");
                options.remove(("key"));
            } else {
                throw new BadClassifierSpecException("Must specify key for model in a sequence file");
            }
        } else if (options.containsKey("file")) {
            Configuration conf = UDFContext.getUDFContext().getJobConf();
            final FSDataInputStream in = FileSystem.get(conf).open(new Path(options.get("file")));
            try {
                model = PolymorphicWritable.read(in, Classifier.class);
            } finally {
                in.close();
            }
            options.remove("file");

        } else {
            throw new BadClassifierSpecException("Must specify existing model");
        }

        if (options.size() > 0) {
            throw new BadClassifierSpecException("Extra options supplied: " + withSpaces.join(options.keySet()));
        }
    }


    /**
     * This callback method must be implemented by all subclasses. This
     * is the method that will be invoked on every Tuple of a given dataset.
     * Since the dataset may be divided up in a variety of ways the programmer
     * should not make assumptions about state that is maintained between
     * invocations of this method.
     *
     * @param input the Tuple to be processed.
     * @return result, of type T.
     * @throws java.io.IOException
     */
    @Override
    public String exec(Tuple input) throws IOException {
        if (model == null) {
            Path sequence = new Path(location);
            Configuration conf = UDFContext.getUDFContext().getJobConf();
            sequence = sequence.makeQualified(FileSystem.get(conf));
            System.out.printf("Model file = %s\n", sequence);
            SequenceFileDirIterator<Text, Classifier> x = new SequenceFileDirIterator<Text, Classifier>(sequence, PathType.GLOB, new PathFilter() {
                public boolean accept(Path path) {
                    System.out.printf("Scanning %s\n", path);
                    return !path.toString().startsWith("_");
                }
            }, null, true, conf);
            int n = 0;
            while (x.hasNext()) {
                Pair<Text, Classifier> pair = x.next();
                n++;
                if (pair.getFirst().toString().equals(key)) {
                    System.out.printf("Found model with categories = %s\n", pair.getSecond().getCategories());
                    model = pair.getSecond();
                    break;
                }
            }
            if (model == null) {
                throw new IOException(String.format("Can't find model with correct key = %s out of %d searched\n", key, n));
            }
            x.close();
        }
        final Vector instance = PigVector.fromBytes(((DataByteArray) input.get(0)));
        final int target = model.getModel().classifyFull(instance).maxValueIndex();
        return model.getCategories().get(target);
    }
}
