import MapReduce
import sys

"""
Relational Join Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: table name
    key = record[0]
    if key == "MovieNames" :
      mr.emit_intermediate( record[2], (record[0], record[1], record[2]) )
    elif key == "MovieRatings" :
      mr.emit_intermediate( record[1], (record[0], record[1], record[2], record[3]) )

def reducer(key, list_of_values):
    # key: unique id in each table used for join
    # value: it is a tuple containing all fields from table including key
    length=len(list_of_values)
    movie_name=list_of_values[0]
    sum=0
    for i in range(1,length):
      movie_rating=list_of_values[i]
      sum=sum+movie_rating[3]
      mr.emit(( movie_name[1], movie_name[2] , movie_rating[1], movie_rating[2], movie_rating[3] ))
    avg=sum/(length-1)
    mr.emit(( movie_name[1],avg ))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
