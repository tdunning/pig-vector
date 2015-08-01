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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;

/**
 * Describes the variables and interactions expressed by an encoding specification.
 */
public class EncodingSpec {
    private Set<Set<String>> variables = Sets.newTreeSet(new Comparator<Set<String>>() {
        public int compare(Set<String> s1, Set<String> s2) {
            Iterator<String> i1 = s1.iterator();
            Iterator<String> i2 = s2.iterator();
            while (i1.hasNext() && i2.hasNext()) {
                int r = i1.next().compareTo(i2.next());
                if (r != 0) {
                    return r;
                }
            }
            if (i2.hasNext()) {
                return -1;
            } else if (i1.hasNext()) {
                return 1;
            } else {
                return 0;
            }
        }
    });

    public EncodingSpec(String variable) {
        variables.add(ImmutableSet.of(variable));
    }

    public EncodingSpec(EncodingSpec v1) {
        this.variables.addAll(v1.getVariables());
    }

    public EncodingSpec(EncodingSpec v1, EncodingSpec v2) {
        this.variables.addAll(v1.getVariables());
        this.variables.addAll(v2.getVariables());
        for (Set<String> x1 : v1.getVariables()) {
            for (Set<String> x2 : v2.getVariables()) {
                Set<String> both = Sets.newTreeSet(x1);
                both.addAll(x2);
                variables.add(both);
            }
        }
    }

    private void add(EncodingSpec spec) {
        variables.addAll(spec.getVariables());
    }

    public Set<Set<String>> getVariables() {
        return variables;
    }

    public static EncodingSpec add(EncodingSpec a, EncodingSpec b) {
        EncodingSpec r = new EncodingSpec(a);
        r.add(b);
        return r;
    }

    public static EncodingSpec interact(EncodingSpec a, EncodingSpec b) {
        return new EncodingSpec(a, b);
    }

    public static EncodingSpec pow(EncodingSpec a, int n) {
        EncodingSpec r = new EncodingSpec(a);
        if (n > r.getVariables().size()) {
            n = r.getVariables().size();
        }
        for (int i = 1; i < n; i++) {
            r = new EncodingSpec(r, a);
        }
        return r;
    }

    public static EncodingSpec atom(String s) {
        return new EncodingSpec(s);
    }
}
