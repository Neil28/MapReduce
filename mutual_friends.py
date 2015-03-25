import MapReduce
import sys

"""
Mutual Friends Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: name of a person
    # value: list of friends
    key = record[0]
    value = record[1]
    for w in value:
      if key>w :
        mr.emit_intermediate(w+key, value)
      else :
        mr.emit_intermediate(key+w, value)

def reducer(key, list_of_values):
    # key: pair of friends
    # value: friend lists
    if len(list_of_values)>1:
         res1=list_of_values[0]
         res2=list_of_values[1]
         #output=list(set(res1) & set(res2))
         output= []
         for i in res1:
           if i  in res2:
               output.append(i)
         if len(output)>0:
            mr.emit((key,output))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
