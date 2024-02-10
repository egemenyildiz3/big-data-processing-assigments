import java.text.SimpleDateFormat
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.functions.PatternProcessFunction

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.CEP.pattern
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import util.Protocol.{Commit, CommitGeo, CommitSummary}
import util.{CommitGeoParser, CommitParser}
import java.util





/** Do NOT rename this class, otherwise autograding will fail. **/
object FlinkAssignment {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {

    /**
      * Setups the streaming environment including loading and parsing of the datasets.
      *
      * DO NOT TOUCH!
      */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // Read and parses commit stream.
    val commitStream =
      env
        .readTextFile("data/flink_commits.json")
        .map(new CommitParser)

    // Read and parses commit geo stream.
    val commitGeoStream =
      env
        .readTextFile("data/flink_commits_geo.json")
        .map(new CommitGeoParser)

    /** Use the space below to print and test your questions. */
//    dummy_question(commitStream).print()
//    question_one(commitStream).print()
//    question_two(commitStream).print()
//    question_three(commitStream).print()
//    question_four(commitStream).print()
//    question_five(commitStream).print()
//    question_six(commitStream).print()
//    question_seven(commitStream).print()
//    question_eight(commitStream, commitGeoStream).print()
    question_nine(commitStream).print()



    /** Start the streaming environment. **/
    env.execute()
  }

  /** Dummy question which maps each commits to its SHA. */
  def dummy_question(input: DataStream[Commit]): DataStream[String] = {
    input.map(_.sha)
  }

  /**
    * Write a Flink application which outputs the sha of commits with at least 20 additions.
    * Output format: sha
    */
  def question_one(input: DataStream[Commit]): DataStream[String] = {
    input
      .filter(commit => commit.stats.isDefined && commit.stats.get.additions>=20)
      .map(commit => commit.sha)
//      input.flatMap{x=>(x.files.map(y=>(x.sha,y.additions)))}.
  }

  def extractFileName(opt: Option[String]): String = {
    opt match {
      case Some(a) => a
      case None => "unknown"
    }
  }

  /**
    * Write a Flink application which outputs the names of the files with more than 30 deletions.
    * Output format:  fileName
    */
  def question_two(input: DataStream[Commit]): DataStream[String] = {

    input
      .flatMap(x=>(x.files.map(y=>(extractFileName(y.filename),y.deletions))))
      .filter(x=>x._2>30)
      .map(x=>x._1)

  }

  /**
    * Count the occurrences of Java and Scala files. I.e. files ending with either .scala or .java.
    * Output format: (fileExtension, #occurrences)
    */
  def question_three(input: DataStream[Commit]): DataStream[(String, Int)] = {

    def extractExtension(fileName: Option[String]): String = {
      fileName match {
        case Some(f) if (f.contains('.') && (f.substring(f.lastIndexOf('.') + 1).equals("scala") || f.substring(f.lastIndexOf('.') + 1).equals("java"))) => f.substring(f.lastIndexOf('.') + 1)
        case _ => ""
      }
    }

//    def fileFilter(filename: String): Boolean = {
//      if (!filename.contains(".")) false
//      else if (!(filename.substring(filename.lastIndexOf('.') + 1).equals("scala") || filename.substring(filename.lastIndexOf('.') + 1).equals("java"))) false
//      else true
//    }

    input
      .flatMap(x => (x.files.map(y => (extractExtension(y.filename)))))
      .filter(x => x!="")
      .map(x=>(x,1))
      .keyBy(0)
      .sum(1)
  }

  /**
    * Count the total amount of changes for each file status (e.g. modified, removed or added) for the following extensions: .js and .py.
    * Output format: (extension, status, count)
    */
  def question_four(input: DataStream[Commit]): DataStream[(String, String, Int)] = {

    def extractExtension(fileName: Option[String]): String = {
      fileName match {
        case Some(f) if (f.contains('.') && (f.substring(f.lastIndexOf('.') + 1).equals("js") || f.substring(f.lastIndexOf('.') + 1).equals("py"))) => f.substring(f.lastIndexOf('.') + 1)
        case _ => ""
      }
    }

    input
      .flatMap(x => (x.files.map(y => (extractExtension(y.filename),y.status.getOrElse(""),y.changes))))
      .filter(x => (x._1 != "" && (x._2!="")))
      .keyBy(0,1)
      .sum(2)

  }

  /**
    * For every day output the amount of commits. Include the timestamp in the following format dd-MM-yyyy; e.g. (26-06-2019, 4) meaning on the 26th of June 2019 there were 4 commits.
    * Make use of a non-keyed window.
   * commit.commit.committer.date
    * Output format: (date, count)
    */
  def question_five(input: DataStream[Commit]): DataStream[(String, Int)] = {

    val dateFormatter = new SimpleDateFormat("dd-MM-yyyy")

    val l = input
      .assignAscendingTimestamps(x=>x.commit.committer.date.getTime)
      .map(x => (dateFormatter.format(x.commit.committer.date),1))
      .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
      .sum(1)

    l
  }

  /**
    * Consider two types of commits; small commits and large commits whereas small: 0 <= x <= 20 and large: x > 20 where x = total amount of changes.
    * Compute every 12 hours the amount of small and large commits in the last 48 hours.
    * Output format: (type, count)
    */
  def question_six(input: DataStream[Commit]): DataStream[(String, Int)] = {

    def smallOrLarge(count: Int): String = {
      if (count>=0 && count<=20) "small"
      else "large"
    }

    input
      .filter(x=>x.stats.isDefined)
      .assignAscendingTimestamps(x=>x.commit.committer.date.getTime)
      .map(x=>(smallOrLarge(x.stats.get.total),1))
      .keyBy(0)
      .window(SlidingEventTimeWindows.of(Time.hours(48), Time.hours(12)))
      .sum(1)

  }

  /**
    * For each repository compute a daily commit summary and output the summaries with more than 20 commits and at most 2 unique committers. The CommitSummary case class is already defined.
    *
    * The fields of this case class:
    *
    * repo: name of the repo.  x.url.split("/")(4) + "/" + x.url.split("/")(5)
    * date: use the start of the window in format "dd-MM-yyyy".
    * amountOfCommits: the number of commits on that day for that repository.
    * amountOfCommitters: the amount of unique committers contributing to the repository.
    * totalChanges: the sum of total changes in all commits.
    * topCommitter: the top committer of that day i.e. with the most commits. Note: if there are multiple top committers; create a comma separated string sorted alphabetically e.g. `georgios,jeroen,wouter`
    *
    * Hint: Write your own ProcessWindowFunction.
    * Output format: CommitSummary
    */
  def question_seven(commitStream: DataStream[Commit]): DataStream[CommitSummary] = {

    def extractRepo(url: String): String = {
      if (!url.contains("/") || url.split("/").length<6) ""
      else url.split("/")(4) + "/" + url.split("/")(5)
    }

    val dateFormatter = new SimpleDateFormat("dd-MM-yyyy")


    commitStream
      .filter(x=>x.stats.isDefined)
      .assignAscendingTimestamps(x=>x.commit.committer.date.getTime)
      .map(x=>(extractRepo(x.url),dateFormatter.format(x.commit.committer.date),x.commit.committer.name, x.stats.get.total))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.days(1)))
      .process(new ProcessWindowFunction[(String,String,String,Int),CommitSummary,String,TimeWindow] {
           override def process(key: String, context: Context, elements: Iterable[(String, String, String, Int)], out: Collector[CommitSummary]): Unit = {

             val repo = key
             val amountOfCommits = elements.size
             val date = dateFormatter.format(context.window.getStart)
             val committers = elements.map(x=>x._3).toList.distinct
             val amountOfCommitters=committers.size
             val totalChanges = elements.map(x=>x._4).sum

             val comCounts = elements
               .map(x => (x._3, 1))
               .groupBy(_._1)
               .map(x=>(x._1,x._2.map(c=>c._2).sum))

             val max = comCounts.maxBy(_._2)._2

             val mostPop = comCounts
               .filter(x => x._2 == max)
               .keys
               .toList
               .sorted

             val mostPopularCommitter = {
               if (mostPop.size==1) mostPop.max
               else {
                 mostPop
                   .reduceLeft((x,y)=>x+","+y)
               }
             }
             val res = CommitSummary(repo, date, amountOfCommits, amountOfCommitters, totalChanges, mostPopularCommitter)
             out.collect(res)
           }
      })
      .filter(x=>(x.amountOfCommitters<=2 && x.amountOfCommits>20))

  }


  /**
    * For this exercise there is another dataset containing CommitGeo events. A CommitGeo event stores the sha of a commit, a date and the continent it was produced in.
    * You can assume that for every commit there is a CommitGeo event arriving within a timeframe of 1 hour before and 30 minutes after the commit.
    * Get the weekly amount of changes for the java files (.java extension) per continent. If no java files are changed in a week, no output should be shown that week.
    *
    * Hint: Find the correct join to use!
    * Output format: (continent, amount)
    */
  def question_eight(commitStream: DataStream[Commit], geoStream: DataStream[CommitGeo]): DataStream[(String, Int)] = {

    def extractExtension(fileName: Option[String]): String = {
      fileName match {
        case Some(f) if (f.contains('.') && f.substring(f.lastIndexOf('.') + 1).equals("java")) => f.substring(f.lastIndexOf('.') + 1)
        case _ => ""
      }
    }

    commitStream
      .assignAscendingTimestamps(x=>x.commit.committer.date.getTime)
      .keyBy(1)
      .intervalJoin(geoStream.assignAscendingTimestamps(x=>x.createdAt.getTime).keyBy(0))
      .between(Time.hours(-1), Time.minutes(30))
      .process(new ProcessJoinFunction[Commit, CommitGeo, (String, Int)] {
        override def processElement(left: Commit, right: CommitGeo, ctx: ProcessJoinFunction[Commit, CommitGeo, (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {

          val count = left.files
            .map(y=>(extractExtension(y.filename), y.changes))
            .filter(x => x._1!="")
            .map(x=>x._2)
            .sum
          val continent = right.continent


          out.collect((continent,count))
        }
      })
      .filter(x=>x._2>0)
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.days(7)))
      .sum(1)

  }

  /**
    * Find all files that were added and removed within one day. Output as (repository, filename).
    *
    * Hint: Use the Complex Event Processing library (CEP).
    * Output format: (repository, filename)
    */
  def question_nine(inputStream: DataStream[Commit]): DataStream[(String, String)] = {

    def extractRepo(url: String): String = {
      if (!url.contains("/") || url.split("/").length < 6) ""
      else url.split("/")(4) + "/" + url.split("/")(5)
    }

    val prep= inputStream
      .assignAscendingTimestamps(commit => commit.commit.committer.date.getTime)
      .flatMap(commit => {
        val repo = extractRepo(commit.url)
        commit.files.map(file => (repo, file.filename, file.status))
      })
      .filter(x => x._1.nonEmpty && x._2.nonEmpty && x._3.nonEmpty)
      .map(x=>(x._1,x._2.get,x._3.get))
      .keyBy(0,1)

    val myPattern = Pattern
      .begin[(String, String, String)]("start")
      .where(x=>x._3=="added")
      .followedBy("end")
      .where(x=>x._3=="removed")
      .within(Time.days(1))

    val patternStream = CEP.pattern(prep, myPattern)

//    patternStream.select(pattern => {
//        val startEvent = pattern.get("start").head
//        val endEvent = pattern.get("end").head
//        (startEvent.head._1, endEvent.head._2)
//      })

//    val result= patternStream.select(new PatternSelectFunction[(String,String,String), (String, String)] {
//       override def select(map: java.util.Map[String, java.util.List[(String,String,String)]], collector: Collector[(String,String)]): Unit = {
//        collector.collect((map.get("start").get(0)._1,map.get("start").get(0)._2))
//      }
//    })

    val result = patternStream.select(new PatternSelectFunction[(String, String, String), (String, String)] {
      override def select(pattern: util.Map[String, util.List[(String, String, String)]]): (String, String) = {
        val startEvent = pattern.get("start").get(0)
        val endEvent = pattern.get("end").get(0)
        (startEvent._1,startEvent._2)
      }
    })

//    val result: DataStream[(String, String)] = patternStream.process(
//      new PatternProcessFunction[(String, String, String), (String, String)]() {
//        override def processMatch(`match`: util.Map[String, util.List[(String, String, String)]],
//                                   ctx: PatternProcessFunction.Context,
//                                   out: Collector[(String, String)]): Unit = {
//
//          val startEvent = `match`("start")(0)
//          val endEvent = `match`("end")(0)
////          val st = `match`("start").head
////          val end = `match`("end").head
////          out.collect(st._1,st._2)
//          out.collect(startEvent._1,endEvent._2)
//        }
//      })


    result

  }

}
