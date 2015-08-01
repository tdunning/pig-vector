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

package org.apache.mahout.pig.encoders;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.Version;
import org.apache.mahout.vectorizer.encoders.*;

import java.lang.reflect.InvocationTargetException;

public class ArgumentEncoder {
    private int position;
    private String name;
    private FeatureVectorEncoder encoder;

    public ArgumentEncoder(int position, String name) {
        this.position = position;
        this.name = name;
    }

    public static ArgumentEncoder newNumericEncoder(int position, String name) {
        ArgumentEncoder r = new ArgumentEncoder(position, name);
        r.encoder = new ContinuousValueEncoder(name);
        return r;
    }

    public static ArgumentEncoder newWordEncoder(int position, String name) {
        ArgumentEncoder r = new ArgumentEncoder(position, name);
        r.encoder = new StaticWordValueEncoder(name);
        return r;
    }

    public static ArgumentEncoder newTextEncoder(int position, String name) {
        ArgumentEncoder r = new ArgumentEncoder(position, name);
        r.encoder = new TextValueEncoder(name);
        return r;
    }

    public static ArgumentEncoder newTextEncoder(int position, String name, String analyzerClass) throws SchemaParseException {
        ArgumentEncoder r = new ArgumentEncoder(position, name);
        LuceneTextValueEncoder enc = new LuceneTextValueEncoder(name);
        Analyzer analyzer;
        try {
            analyzer = (Analyzer) Class.forName(analyzerClass).getConstructor(Version.class).newInstance(Version.LUCENE_31);
        } catch (InstantiationException e) {
            throw new SchemaParseException("Can't construct analyzer for class " + analyzerClass, e);
        } catch (IllegalAccessException e) {
            throw new SchemaParseException("Can't access analyzer class " + analyzerClass, e);
        } catch (ClassNotFoundException e) {
            throw new SchemaParseException("Can't find analyzer class " + analyzerClass, e);
        } catch (NoSuchMethodException e) {
            throw new SchemaParseException("Can't find constructor for analyzer class " + analyzerClass, e);
        } catch (InvocationTargetException e) {
            throw new SchemaParseException("Can't construct analyzer object " + analyzerClass, e);
        }
        enc.setAnalyzer(analyzer);
        r.encoder = enc;
        return r;
    }

    public FeatureVectorEncoder getEncoder() {
        return encoder;
    }

    public int getPosition() {
        return position;
    }
}
