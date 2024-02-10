package dataset

import dataset.util.XMLDatafile.Badge

import scala.io.Source


/**
 * PART 5A - DATASET2 / StackOverFlow Badges
 *
 * In this assignment you will be asked to finish reading in a not quite xml file
 * The file is one big list of lines such asked
 *
 * <row Id="1" UserId="3" Name="Autobiographer" Date="2012-03-06T18:53:16.300" Class="3" TagBased="False" />
 *
 * For this assignment you first have to prep your data a bit, and then it's 
 * on to answering questions. This part is worth 9 points
 */
object Part5B {

  /** Q28 (3p)
   * Included is a first example of reading in a file:
   * `sourceAsListString` generates a `List[String]`
   *
   * We would like you to finish `source` converting this into a List of
   * case class Badge, i.e. make sure the return type for source is
   * `List[Badge], you can find Badge in the util folder
   */
  val sourceAsListString = Source.fromResource("First6200Badges.xml").getLines.toList.drop(2).dropRight(1)

  val source: List[Badge] = Source.fromResource("First6200Badges.xml").getLines().toList.drop(2).dropRight(1)
  	.map(line => {
      val valuesList = line.trim.split('"').zipWithIndex.filter(
        pair => {pair._2 % 2 == 1 }
      ).map(pair => {pair._1})

      Badge(
        valuesList.apply(0).toInt,
        valuesList.apply(1).toInt,
        valuesList.apply(2),
        valuesList.apply(3),
        valuesList.apply(4).toInt,
        valuesList.apply(5) == "True"
      )
    })

  /**
   * Again you can use this to get some output
   */
  def main(args: Array[String]): Unit = {
    //println(showResults(sourceAsListString))
  }

  def showResults(input: List[String]): Unit = input.foreach(println)


  /** Q29 (3p)
   *
   * What is the easiest attainable badge? Output a tuple of its name and nr
   */
  def easiestAttainableBadge(input: List[Badge]): (String, Int) = {
    input.map(_.name).groupBy(identity).mapValues(_.size).maxBy(pair => pair._2)
  }

  /** Q30 (3p)
   *
   * Return a tuple of tuples of the least productive and most productive
   * year, together with the nr of badges earned
   */
  def yearOverview(input: List[Badge]): ((Int, Int), (Int, Int)) = {
    val yearlyProductivity = input.map(_.badgeDate).map(_.split('-').apply(0).toInt).groupBy(identity).mapValues(_.size).toList

    (yearlyProductivity.minBy(_._2), yearlyProductivity.maxBy(_._2))
  }

  // END OF PART 5B

}
