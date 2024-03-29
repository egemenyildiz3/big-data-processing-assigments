package DataFrameAssignment


import java.sql.Timestamp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import utils.File

/**
 * Note read the comments carefully, as they describe the expected result and may contain hints in how
 * to tackle the exercises. Note that the data that is given in the examples in the comments does
 * reflect the format of the data, but not the result the graders expect (unless stated otherwise).
 */
object DFAssignment {


  /**
   *                                     Description
   *
   * To get a better overview of the data, we want to see only a few columns out of the data. We want to know
   * the committer name, the timestamp of the commit and the length of the message in that commit
   *
   *                                      Output
   *
   *
   * | committer      | timestamp            | message_length |
   * |----------------|----------------------|----------------|
   * | Harbar-Inbound | 2019-03-10T15:24:16Z | 1              |
   * | ...            | ...                  | ...            |
   *
   *                                       Hints
   *
   * Try to work out the individual stages of the exercises, which makes it easier to track bugs,
   * and figure out how Spark Dataframes and their operations work.
   *
   * You can also use the `printSchema()` function and `show()` function to take a look at the structure
   * and contents of the Dataframes.
   *
   * For mapping values of a single column, look into user defined functions (udf)
   *
   * @param commits Commit Dataframe, created from the data_raw.json file.
   * @return DataFrame of commits including the commit timestamp
   *         and the length of the message in that commit.
   */
  def assignment_12(commits: DataFrame): DataFrame = {
    commits
      .select(
        col("commit.committer.name").alias("committer"),
        col("commit.committer.date").alias("timestamp"),
        length(col("commit.message")).alias("message_length")
      )
  }

  /**
   *                                    Description
   *
   * In this exercise we want to know all the commit SHAs from a list of commit committers.
   * We want to order them by the committer names alphabetically.
   *
   *                                      Output
   *
   * | committer      | sha                                      |
   * |----------------|------------------------------------------|
   * | Harbar-Inbound | 1d8e15a834a2157fe7af04421c42a893e8a1f23a |
   * | ...            | ...                                      |
   *
   * @param commits Commit Dataframe, created from the data_raw.json file.
   * @param committers Sequence of String representing the authors from which we want to know their respective commit
   *                SHAs.
   * @return DataFrame of commits from the requested authors including the commit SHA.
   */
  def assignment_13(commits: DataFrame, committers: Seq[String]): DataFrame = {
    commits
      .select(
        col("commit.committer.name").alias("committer"),
        col("sha")
      )
      .filter(col("committer").isin(committers:_*))
      .orderBy(asc("committer"))

  }


  /**
   *                                   Description
   *
   * We want to generate yearly dashboards for all users, per each project they contribute to.
   * In order to achieve that, we need the data to be partitioned by years.
   *
   *
   *                                      Output
   *
   * | repository | committer        | year | count   |
   * |------------|------------------|------|---------|
   * | Maven      | magnifer         | 2019 | 21      |
   * | .....      | ..               | .... | ..      |
   *
   * @param commits Commit Dataframe, created from the data_raw.json file.
   * @return Dataframe containing 4 columns, Repository name, committer name, year
   *         and the number of commits for a given year.
   */
  def assignment_14(commits: DataFrame): DataFrame = {
    commits
      .withColumn("repository", split(col("url"), "/")(5))
      .select(
        col("repository"),
        col("commit.committer.name").alias("committer"),
        year(col("commit.committer.date")).alias("year")
      )
      .groupBy("repository", "committer", "year")
      .agg(
        count("*").alias("count")
      )

  }


  /**
   *                                        Description
   *
   * A developer is interested in what day of the week some commits are pushed. Extend the DataFrame
   * by determining for each row the day of the week the commit was made on.
   *
   *                                          Output
   *
   * | day    |
   * |--------|
   * | Mon    |
   * | Fri    |
   * | ...    |
   *
   *                                           Hints
   *
   * Look into SQL functions in for Spark SQL.
   *
   * @param commits Commit Dataframe, created from the data_raw.json file.
   * @return the inputted DataFrame appended with a day column.
   */
  def assignment_15(commits: DataFrame): DataFrame = {
    commits.
      withColumn("day",
        when(dayofweek(col("commit.committer.date")) === 1, "Sun")
          .when(dayofweek(col("commit.committer.date")) === 2, "Mon")
          .when(dayofweek(col("commit.committer.date")) === 3, "Tue")
          .when(dayofweek(col("commit.committer.date")) === 4, "Wed")
          .when(dayofweek(col("commit.committer.date")) === 5, "Thu")
          .when(dayofweek(col("commit.committer.date")) === 6, "Fri")
          .when(dayofweek(col("commit.committer.date")) === 7, "Sat")
      )
  }


  /**
   *                                            Description
   *
   * We want to know how often some committers commit, and more specifically, what are their time intervals
   * between their commits. To achieve that, for each commit, we want to add two columns:
   * the column with the previous commits of that user and the next commit of that user. The dates provided should be
   * independent from depository - if a user works on a few repositories at the same time, the previous date or the
   * next date can be from a different repository.
   *
   *                                              Output
   *
   *
   * | $oid                     	| prev_date   	           | date                     | next_date 	             |
   * |--------------------------	|--------------------------|--------------------------|--------------------------|
   * | 5ce6929e6480fd0d91d3106a 	| 2019-01-03T09:11:26.000Z | 2019-01-27T07:09:13.000Z | 2019-03-04T15:21:52.000Z |
   * | 5ce693156480fd0d5edbd708 	| 2019-01-27T07:09:13.000Z | 2019-03-04T15:21:52.000Z | 2019-03-06T13:55:25.000Z |
   * | 5ce691b06480fd0fe0972350 	| 2019-03-04T15:21:52.000Z | 2019-03-06T13:55:25.000Z | 2019-04-14T14:17:23.000Z |
   * | ...                      	| ...    	                 | ...                      | ...       	             |
   *
   *                                               Hints
   *
   * Look into Spark sql's Window to have more expressive power in custom aggregations
   *
   *
   * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
   *                `println(commits.schema)`.
   * @param committerName Name of the author for which the result must be generated.
   * @return DataFrame with a columns `$oid` , `prev_date`, `date` and `next_date`
   */
  def assignment_16(commits: DataFrame, committerName: String): DataFrame = {

    val windowInstance = Window.orderBy("commit.committer.date")

    // Filtering the comments by a given committerName and adding the $oid column
    val filteredCommits = commits.filter(lower(col("commit.committer.name")) === committerName.toLowerCase())
      .withColumn("$oid", col("_id.$oid"))


    // Adding previous and next date columns using windows
    val headingAndTrailingDatesCommits = filteredCommits
      .withColumn("prev_date", lag(col("commit.committer.date"), 1).over(windowInstance))
      .withColumn("next_date", lead(col("commit.committer.date"), 1).over(windowInstance))

    headingAndTrailingDatesCommits
      .select(
        col("$oid"),
        col("prev_date"),
        col("commit.committer.date").alias("date"),
        col("next_date")
      )

  }


  /**
   *
   *                                           Description
   *
   * After looking at the results of assignment 5, you realise that the timestamps are somewhat hard to read
   * and analyze easily. Therefore, you now want to change the format of the list.
   * Instead of the timestamps of previous, current and next commit, output:
   *      - Timestamp of the current commit  (date)
   *      - Difference in days between current commit and the previous commit (days_diff)
   *      - Difference in minutes between the current commit (minutes_diff [rounded down])
   *      - Current commit (Oid)
   *
   * For both fields, i.e. the difference in days and difference in minutes, if the value is null
   * replace it with 0. When there is no previous commit, the value should be 0.
   *
   *
   *                                             Output
   *
   *
   * | $oid                        | date                     | days_diff 	| minutes_diff |
   * |--------------------------	|--------------------------	|-----------	|--------------|
   * | 5ce6929e6480fd0d91d3106a 	| 2019-01-27T07:09:13.000Z 	| 0         	| 3            |
   * | 5ce693156480fd0d5edbd708 	| 2019-03-04T15:21:52.000Z 	| 36        	| 158          |
   * | 5ce691b06480fd0fe0972350 	| 2019-03-06T13:55:25.000Z 	| 2         	| 22           |
   * | ...                      	| ...                      	| ...       	| ...          |
   *
   *                                              Hints
   *
   * Look into Spark sql functions. Days difference is easier to calculate than minutes difference.
   *
   * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
   *                `println(commits.schema)`.
   * @param committerName Name of the author for which the result must be generated.
   * @return DataFrame with columns as described above.
   */
  def assignment_17(commits: DataFrame, committerName: String): DataFrame = {
    // TODO: Needs fixing
    /*

    val windowInstance = Window.orderBy(col("date"))

    // Filter out the columns with a different committerName and add the $oid column and select only needed columns
    val filteredAndCleanedCommits = commits
      .filter(lower(col("commit.author.name")) === committerName.toLowerCase())
      .withColumn("$oid", col("_id.$oid"))
      .select(
        col("$oid"),
        col("commit.committer.date") alias "date"
      )
      .sort(col("date").asc)

    filteredAndCleanedCommits.show()

    // Added timestamp casted date column to be used for calculations and find previous commit
    val timestampsAndPreviousIncludedCommits = filteredAndCleanedCommits
      .withColumn("date_casted", col("date").cast("timestamp"))
      .withColumn("prev_commit", lag(col("date_casted"), 1).over(windowInstance))

    // Just calulcating time difference in days and minutes
    val timeDifferenceIncludedCommits = timestampsAndPreviousIncludedCommits
      .withColumn(
        "days_diff",
        when(
          col("prev_commit").isNotNull,
          datediff(
            col("date_casted"),
            col("prev_commit")
          )
        ).otherwise(0).cast("int")
      )
      .withColumn(
        "minutes_diff",
        when(
          col("prev_commit").isNotNull,
          floor(
            (unix_timestamp(col("date_casted")) - unix_timestamp(col("prev_commit"))) / 60
          )
        ).otherwise(0).cast("int")
      )

    // Filling null values with 0
    val filledCommits = timeDifferenceIncludedCommits.na.fill(0, Seq("days_diff", "minutes_diff"))

    // Select required columns
    filledCommits.select("$oid", "date", "days_diff", "minutes_diff")
    */

    val windowInstance = Window
      .orderBy("date")


    val filteredCommits = commits.filter(col("commit.author.name") === committerName)

    val cleanedCommits = filteredCommits.select(
      col("_id.$oid").alias("$oid"),
      col("commit.author.date").alias("date")
    )

    val timeDifferenceIncludedCommits = cleanedCommits
      .withColumn(
        "days_diff",
        datediff(col("date"), lag("date", 1).over(windowInstance))
      )
      .withColumn(
        "minutes_diff",
        floor((to_timestamp(col("date")).cast("int") - to_timestamp(lag("date", 1).over(windowInstance)).cast("int")) / 60)
      )

    timeDifferenceIncludedCommits.na.fill(0)



  }



  /**
   *                                        Description
   *
   * To get a bit of insight in the spark SQL, and its aggregation functions, you will have to
   * implement a function that returns a DataFrame containing columns:
   *        - repository
   *        - month
   *        - commits_per_month(how many commits were done to the given repository in the given month)
   *
   *                                          Output
   *
   *
   * | repository   | month | commits_per_month |
   * |--------------|-------|-------------------|
   * | OffloadBuddy | 1     | 32                |
   * | ...          | ...   | ...               |
   *
   * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
   *                `println(commits.schema)`.
   * @return DataFrame containing a `repository` column, a `month` column and a `commits_per_month`
   *         representing a count of the total number of commits that that were ever made during that month.
   */
  def assignment_18(commits: DataFrame): DataFrame = {

    // Adding a column for a repository and month
    val repositoryAndMonthIncludedCommits = commits
      .withColumn("repository", split(col("url"), "/")(5))
      .withColumn("month", month(col("commit.committer.date")))

    // Grouping by all and counting
    repositoryAndMonthIncludedCommits
      .groupBy("repository", "month")
      .agg(count("*").alias("commits_per_month"))

  }

  /**
   *                                        Description
   *
   * In a repository, the general order of commits can be deduced from  timestamps. However, that does not say
   * anything about branches, as work can be done in multiple branches simultaneously. To trace the actual order
   * of commits, using commits SHAs and Parent SHAs is necessary. We are interested in commits were a parent commit
   * has a different committer than the child commit.
   *
   * Output a list of committer names, and the number of times this happened.
   *
   *                                          Output
   *
   *
   * | parent_name | times_parent |
   * |-------------|--------------|
   * | Emeric      | 2            |
   * | ...         | ...          |
   *
   * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
   *                `println(commits.schema)`.
   * @return DataFrame containing the parent name and the count for the parent.
   */
  def assignment_19(commits: DataFrame): DataFrame = {

    // Form a secondary commits data frame with only columns needed
    val secondaryCommits = commits
      .select(
        col("sha").alias("parent_sha"),
        col("commit.committer.name").alias("parent_name")
      )

    // Select only needed columns
    val cleanedCommits = commits
      .select(
        col("sha"),
        col("commit.committer.name").alias("child_name"),
        col("parents.sha").alias("parents_sha")
      )

    // Explode lists in parents_sha columns
    val explodedCommits = cleanedCommits
      .withColumn("parent_sha", explode(col("parents_sha")))
      .select(
        col("sha"),
        col("child_name"),
        col("parent_sha")
      )

    // Join main and secondary dataframe on parent_sha
    val joinedCommits = explodedCommits.join(secondaryCommits, "parent_sha")

    // Filter out when child_name != parent_name
    val unmatchedCommits = joinedCommits
      .filter(col("child_name") =!= col("parent_name"))

    // Group and count
    unmatchedCommits
      .groupBy("parent_name")
      .agg(count("*").alias("times_parent"))

  }

}
