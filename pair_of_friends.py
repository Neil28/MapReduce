import MapReduce
import sys

"""
Pair of Friends Example in the Simple Python MapReduce Framework
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
        mr.emit_intermediate(w+" "+key, 1)
      else :
        mr.emit_intermediate(key+" "+w, 1)

def reducer(key, list_of_values):
    # key: pair of friends with spaces in between
    # value: list of occurrence counts
    total = 0
    for v in list_of_values:
      total += v
    if total==2: 
      val=key.split()    
      mr.emit((val[0], val[1]))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
