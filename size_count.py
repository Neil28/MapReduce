import MapReduce
import sys

"""
Size Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
  # key: document identifier
  # value: document contents
  key = record[0]
  value = record[1]
  words = value.split()
  tiny=small=medium=large=0
  for w in words:
     if len(w)==1 :  
         tiny=tiny+1
     elif len(w)>=2 and len(w)<=4 :
         small=small+1
     elif len(w)>=5 and len(w)<=9 :
         medium=medium+1
     elif len(w)>=10 :
         large=large+1
  mr.emit_intermediate(key,('tiny', tiny))
  mr.emit_intermediate(key,('small', small))
  mr.emit_intermediate(key,('medium', medium))
  mr.emit_intermediate(key,('large', large))

def reducer(key, list_of_values):
    # key: document identifier
    # value: type of word and its count
    tiny_count=small_count=medium_count=large_count=0
    for v in list_of_values:
      if v[0]=='tiny' :
         tiny_count+=v[1]
      if v[0]== 'small' :
         small_count+=v[1]
      if v[0]== 'medium' :
         medium_count+=v[1]
      if v[0]== 'large' :
         large_count+=v[1]
    mr.emit((key,[('large', large_count),('medium', medium_count),('small', small_count),('tiny', tiny_count)]))
   

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
