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

    define EncodeVector org.apache.mahout.pig.EncodeVector('10','x+y+1', 'x:numeric, y:numeric');

The other important type is text.  If you just say "text" is the type, then whitespace is used for tokenization.  You can also add a Lucene 3.1 tokenizer in parentheses if you want something fancier.  There is also a word data type.

Then we just need data to encode.  Here I read an output file that has some silly data in it.

    a = load '/Users/tdunning/Downloads/NNBench.csv' using PigStorage(',') as (x1:int, x2:int, x3:int);

and here I encode all of the data (that I care about):

    b = foreach a generate EncodeVector(*);

You can look at the output

    dump b;         

Or you can write it out in binary format

    store b into 'foo.bin' using BinStorage;

Send questions and suggestions to ted.dunning@gmail.com

