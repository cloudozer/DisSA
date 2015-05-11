# DisSA
Distributed Suffix Array Construction

## Overview
A distributed suffix array construction algorithm allows to build a suffix array for a long strings using divide and conquer approach. Each chunk of a string is a separate task that can take any available worker.

## Input and Output data
It is assumed that data files containing sequences should be put into "data/" folder for master node as well as for worker node.
Output data - a suffix array is constructed on master node.

## Running on a laptop
Start Erlang shell. Then run

  > master:main(FileName, Worker_nbr).
  
Where Filename is a name of the file containing a given sequence. Worker_nbr is a number of workers which concurrently perform suffix array construction. You may choose any number from 1 to the number of cores your laptop has got or even more.


## Running in cloud
TODO


