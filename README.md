This code provides the ability to encode data in Pig using the hashed encoding capabilities of Mahout.

To try this out, start by compiling and installing mahout in your local repository:

    cd ~/Apache
    git clone https://github.com/apache/mahout.git
    cd mahout
    mvn install -DskipTests

Then do the same with pig-vector

    cd ~/Apache
    git clone git@github.com:tdunning/pig-vector.git
    cd pig-vector
    mvn package

Now you have a comprehensive jar that includes pig-vector and Mahout.  This jar is big (24M) but it has a lot of stuff on its mind.

Now you can write a program in pig that uses Mahout vectors.

To do this, start off by registering the pig-vector jar:

    register 'target/pig-vector-1.0-jar-with-dependencies.jar';

Then define an encoding function.  The arguments to the constructor are 

- the dimension of the resulting vectors (100,000 to 1,000,000 is a more common range than the 10 showed here)

- a description of the variables to be included in the encoding.    The interactions currently don't work, but all the rest does.

- the schema of the tuples that pig will pass together with their data types.

There is a lot of flexibility here in terms of tokenizing text, reusing fields with different encodings and interaction variables.

    define EncodeVector org.apache.mahout.pig.EncodeVector('10','x+y+1', 'x:numeric, y:numeric, z:numeric');

The other important types of data are word (for categorical data) and text.  If you just say "text" is the type, then
whitespace is used for tokenization.  You can also add a Lucene 3.1 tokenizer in parentheses if you want something fancier
in the way of tokenization.  The encoder treats word fields as strings with no processing whatsoever.

The formula is not a normal arithmetic formula, but is more akin to an R formula as used in regression.  In this style,
you list the variables you want to include, joined by +'s.  You can add 1 if you want a constant terms which is very important
for most generalized linear classifiers.  You can specify interactions using * or you can add all interactions of a group
of terms by using exponentiation.  That allows something like (x+y+z)^3 which includes all first, second and third order
variables.  The syntax doesn't yet allow the ':' operator of R and when it comes time to encoding no interactions are implemented
yet, regardless of syntax support.

Then we just need data to encode.  Here I read an output file that has some silly data in it.

    a = load '/Users/tdunning/Downloads/NNBench.csv' using PigStorage(',') as (x1:int, x2:int, x3:int);

and here is code that encodes all of the data (that we care about):

    b = foreach a generate EncodeVector(*);

Note that the schema for the tuple has fields x1, x2, x3 while the schema for the encoding has x, y, z.
Since the schema of the tuple is not visible to the UDF, we can't always avoid this kind of mismatch except by careful
coding of the pig code.  Note also how the encoding of the vector (defined by the formula "x+y+1") includes fields x1
and x2 and a bias term but excludes x3.

You can look at the output

    dump b;         

The output should have two non-zero fields that encode x1, two that encode x2 and a single field that encodes the bias.
With only 10 dimensional output vectors, some of these may overlap and cause a larger value.

Or you can write it out in binary format

    store b into 'foo.bin' using BinStorage;

Send questions and suggestions to ted.dunning@gmail.com

