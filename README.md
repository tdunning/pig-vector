**This package should be considered deprecated at this point and unmaintained.**

This code provides the ability to encode data in Pig using the hashed encoding capabilities of Mahout.  It also allows
training of logistic regression models.

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

    define EncodeVector org.apache.mahout.pig.encoders.EncodeVector('10','x+y+1', 'x:numeric, y:numeric, z:numeric');

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

    b = foreach a generate 1 as key, EncodeVector(*) as v;

Note that the schema for the tuple has fields x1, x2, x3 while the schema for the encoding has x, y, z.
Since the schema of the tuple is not visible to the UDF, we can't always avoid this kind of mismatch except by careful
coding of the pig code.  Note also how the encoding of the vector (defined by the formula "x+y+1") includes fields x1
and x2 and a bias term but excludes x3.

You can look at the output

    dump b;         

The output is in binary form so you can't see it, but it has two non-zero fields that encode x1, two that
encode x2 and a single field that encodes the bias. With only 10 dimensional output vectors, some of these may
overlap and cause a larger value.

Or you can write the vector out in a sequence file by using the elephant-bird package from twitter which is
included in pig-vector:

    store b into 'vectors.dat' using com.twitter.elephantbird.pig.store.SequenceFileStorage (
       '-c com.twitter.elephantbird.pig.util.IntWritableConverter',
       '-c com.twitter.elephantbird.pig.util.GenericWritableConverter -t org.apache.mahout.math.VectorWritable'
    );

This data isn't very interesting because the key for every vector is the constant value 1, but you can see the
pattern.  Note how the IntWritableConverter is used to specify the type of the key and the GenericWritableConverter
is used to specify how to write the vector value.  The -t option is required so that we can specify what the
actual type of is for the data we want to write.

Vectors encoded using DirectVector or EncodeVector are passed around in pig as bytearray's.  If they are passed
to a learning algorithm, they have to be converted into real vector objects in order to be passed to the learning
algorithm.  When writing them out to a file, we can just use the bytes as is with the help of the
GenericWritableConverter class.

To train a model, we use the org.apache.mahout.pig.LogisticRegression UDF.  The constructor for this function
requires a set of options to specify how it should run.  Here is a sample:

    define train org.apache.mahout.pig.LogisticRegression('iterations=5, inMemory=true, features=100000, categories=alt.atheism comp.sys.mac.hardware rec.motorcycles sci.electronics talk.politics.guns comp.graphics comp.windows.x rec.sport.baseball sci.med talk.politics.mideast comp.os.ms-windows.misc misc.forsale rec.sport.hockey sci.space talk.politics.misc comp.sys.ibm.pc.hardware rec.autos sci.crypt soc.religion.christian talk.religion.misc');

This very long line specifies the number of learning iterations (5), that the training vectors should be kept
in memory and that the feature vectors are have 100,000 elements.  The categories option is a space delimited
list of the categories and must match the strings that are passed into the learning algorithm as the target variable.

Here is a complete training script:

    register 'target/pig-vector-1.0-jar-with-dependencies.jar';
    define encodeVector org.apache.mahout.pig.encoders.EncodeVector('100000', 'subject+body', 'group:word, article:numeric, subject:text, body:text');
    define train org.apache.mahout.pig.LogisticRegression('iterations=5, inMemory=true, features=100000, categories=alt.atheism comp.sys.mac.hardware rec.motorcycles sci.electronics talk.politics.guns comp.graphics comp.windows.x rec.sport.baseball sci.med talk.politics.mideast comp.os.ms-windows.misc misc.forsale rec.sport.hockey sci.space talk.politics.misc comp.sys.ibm.pc.hardware rec.autos sci.crypt soc.religion.christian talk.religion.misc');

    rm model.dat

    /* read the data */
    docs = load '20news-bydate-train/*/*' using org.apache.mahout.pig.MessageLoader()
         as (newsgroup, id:int, subject, body);
    /* encode as vectors, retain the target variable and the feature vector */
    vectors = foreach docs generate newsgroup, encodeVector(*) as v;
    describe vectors;

    /* put the training data in a single bag.  We could train multiple models this way */
    grouped = group vectors all;

    /* train the actual model.  The key is bogus to satisfy the sequence vector format. */
    model = foreach grouped generate 1 as key, train(vectors) as model;

    /* the trained model is passed to use as a bytearray so we just pass it on out.  The classifier
       class just contains the list of target valeus and the OnlineLogisticRegression object itself. */
    store model into 'model.dat' using com.twitter.elephantbird.pig.store.SequenceFileStorage (
       '-c com.twitter.elephantbird.pig.util.IntWritableConverter',
       '-c com.twitter.elephantbird.pig.util.GenericWritableConverter -t org.apache.mahout.pig.Classifier'
    );

Send questions and suggestions to ted.dunning@gmail.com

