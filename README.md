# Matrix Multiplication on MapReduce 

# Description 
 The purpose of this project is to develop a MapReduce program on Hadoop to multiply two sparse matrices. 
# Input
There are two small sparse matrices in the files M-matrix-small.txt and N-matrix-small.txt for testing in standalone mode. Their matrix multiplication must return the result matrix in solution-small.txt.  

# Pseudo-Code 

class Elem extends Writable {   
short tag;  // 0 for M, 1 for N   
int index;  // one of the indexes (the other is used as a key)   
double value;   
... 
}  

class Pair extends WritableComparable<Pair> {   
int i;   
int j;   
... 
}

#### First Map-Reduce job:
                            
map(key,line) =          // mapper for matrix M                         
    split line into 3 values: i, j, and v                                  
    emit(j,new Elem(0,i,v))

map(key,line) =    // mapper for matrix N                                                                                                                              
    split line into 3 values: i, j, and v                                                                                                                              
    emit(i,new Elem(1,j,v))

reduce(index,values) =                                            
A = all v in values with v.tag==0                                                                                     
B = all v in values with v.tag==1                                                                                       

for a in A                                                                                                           
    for b in B                                                                                      
        emit(new Pair(a.index,b.index),a.value*b.value)

#### Second Map-Reduce job:
map(key,value) = // do nothing                                                                     
  emit(key,value)                                                                             

reduce(pair,values) = // do the summation                                                                           
m = 0                                                                                                         
for v in values                                                                                     
m = m+v                                                                                                                          
emit(pair,pair.i+","+pair.j+","+m)                                                                 

# To compile and run the project on your laptop: 

cd HadoopMatMult 

mvn install rm -rf intermediate output 

~/hadoop-3.3.2/bin/hadoop jar target/*.jar Multiply M-matrix-small.txt N-matrix-small.txt intermediate output 

# Output 

The file output/part-r-00000 will contain the results which must be the same as in solution-small.txt  
