from pyspark import SparkConf, SparkContext

def unique_pairs(text):
    lines = text.split("\n")
    for line in lines:
        disct_words = []
        words = line.split(" ")
        for word in words:
          if word not in disct_words:
             disct_words.append(word)
    #for i in range(len(disct_words)):
    #    for j in range(i+1,len(disct_words)):
    return disct_words


conf= SparkConf().setMaster("local").setAppName("Test_App")
sc= SparkContext(conf= conf)

lines_rdd = sc.textFile("file.txt")
unique_pairs_rdd = lines_rdd.flatMap(unique_pairs)
unique_pairs_rdd.saveAsTextFile("output")
