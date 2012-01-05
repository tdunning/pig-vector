register 'target/pig-vector-1.0-jar-with-dependencies.jar';
define EncodeVector org.apache.mahout.pig.encoders.EncodeVector('10','x+y+1', 'x:numeric, y:numeric');
define DirectVector org.apache.mahout.pig.encoders.DirectVector('x1,x2,x3');

a = load '/Users/tdunning/Downloads/NNBench.csv' using PigStorage(',') as (x1:int, x2:int, x3:int);
b = foreach a generate EncodeVector(*), DirectVector(*);
dump b;         
store b into 'foo.bin' using BinStorage;
