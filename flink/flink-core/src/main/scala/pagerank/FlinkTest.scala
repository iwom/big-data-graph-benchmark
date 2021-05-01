package pagerank

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


object FlinkTest extends App {
  case class Page(id: Long, rank: Double)

  case class Adjacency(id: Long, neighbours: Array[Long])

  type FilePath = String

  override def main(args: Array[String]): Unit = {
    val filePath: FilePath = args(0)
    val numIterations: Int = args(1).toInt

    val env = ExecutionEnvironment.getExecutionEnvironment
    pageRank(env, filePath, numIterations)
  }

  def pageRank(env: ExecutionEnvironment, filePath: FilePath, numIterations: Int): Unit = {
    val edges = env.readTextFile(filePath)
      .map(line => {
        val elements = line.split("\\s+")
        (elements(0).toLong, elements(1).toLong)
      })
      .distinct()

    val pages = edges
      .flatMap(edge =>
        Seq(Page(edge._1, 1.0), Page(edge._2, 1.0))
      )
      .distinct()

    val initialRanks = pages.map(page => Page(page.id, 1.0))

    val randomJump = 0.15

    val adjacency = edges
      .map(e => Adjacency(e._1, Array(e._2)))
      .groupBy(_.id)
      .reduce { (l1, l2) => Adjacency(l1.id, l1.neighbours ++ l2.neighbours) }

    val iteration = initialRanks.iterate(numIterations) {
      pages => {
        val contribs = pages
          .join(adjacency)
          .where("id")
          .equalTo("id") {
            (page, adjacency, out: Collector[Page]) => {
              val rankPerTarget = 0.85 * page.rank / adjacency.neighbours.length.toDouble
              out.collect(Page(page.id, randomJump))
              for (neighbour <- adjacency.neighbours) {
                out.collect(Page(neighbour, rankPerTarget))
              }
            }
          }
        contribs.groupBy("id").reduce((a, b) => Page(a.id, a.rank + b.rank))
      }
    }
    iteration.print()
  }
}
