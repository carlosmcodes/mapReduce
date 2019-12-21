package spark
import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.StdIn

object Main {

  def main(args: Array[String]): Unit = {
    println("hello world")
    sparkFunctions()
  }
  def halt(): Unit = {
    println("Both jobs are done. Press any key to exit.")
    StdIn.readLine()
  }

  def sparkFunctions(): Unit = {
    val conf = new SparkConf().setAppName("mapreduceSpark").setMaster("local[*]")
    val sparkContext = new SparkContext(conf)
//    val textFile = sparkContext.textFile("hdfs://...")
    val textFile = sparkContext.textFile("../../Documents/sfsu/csc847/maildir/*/*/*/*/*/*/*")
//    val textFile = sparkContext.textFile("gs://jobs112/maildir/**")
    val preProcessing = textFile.flatMap(line => line.split(" ")).map(x => (x,1))
      .filter(k => removeNonWords(k._1)).reduceByKey(_ + _)
      .reduce((a,b) => (a._1 + " " + b._1, 0))
      ._1.split( " ")
    val scrubbedRDD = sparkContext.parallelize(preProcessing)
    val anagramMapReduce = scrubbedRDD.map(anagramMap).reduceByKey(anagramReduceKey).filter( k => k._2._1 > 1).reduce(anagramFinalReduce)._2._2
    val palindroneMapReduce = scrubbedRDD.map(x => (x,checkPalindrone(x))).filter(k => k._2).reduce((a,b) => (a._1+ " " + b._1, true))._1
    var anagramRDD = sparkContext.parallelize(anagramMapReduce.split(" "))
    anagramRDD.saveAsTextFile("ANAGRAMRR.TXT")
    writeToFile("anagram.txt", anagramMapReduce)
    writeToFile("palindrone.txt", palindroneMapReduce)
    halt()
  }
  def writeToFile(file: String, txt: String): Unit = {
    val pw = new PrintWriter(new File(file ))
    pw.write(txt+ "\n")
    pw.close()
  }
  val anagramMap : String => (String, (Int, String)) = {x => (sortedChars(x), (1,x))}
  val anagramReduceKey : ((Int, String),(Int, String)) => (Int, String) = {(a,b) => (a._1 + b._1,"("+  a._2 + " " + b._2+")"  )}
  val anagramFinalReduce : ((String, (Int, String)),(String, (Int, String)))
    => (String, (Int, String)) = {(a,b)
    => ("",(0, a._2._2 +" "+ b._2._2))}

  def removeNonWords(s: String): Boolean = if (s.matches("^[a-zA-Z]*")) true else false
  def sortedChars(word: String): String = word.toCharArray.sortWith(_ < _).mkString
  def checkPalindrone(word: String): Boolean = if (word.reverse == word && word.length > 1)  true else false
}

