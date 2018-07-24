# parallel_sort2d
Sorting a 2D array in a distributed memory environment using OpenMPI
(It is assumed that each node has a shared disk memory).
The input specification is:
- The code is run as 
`mpirun -np (number of nodes) (ips of the hosts) sort2d (number of columns) (path to columns/col)`
The files in the path are named as col1, col2, ... The output is stored in a row major form as col0.
- All the column files and the output files are binary files.
- The column files start with an integer which denote the size of all the strings in the column elements of that particular file, followed by (float, string) pairs.

The array is read in a parallel fashion with each node reading it's alloted columns.
