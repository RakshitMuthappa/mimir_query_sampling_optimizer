package mimir.optimizer.operator

import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.algebra._
import mimir.optimizer._

case class DecomposedAggregate(postprocess: ProjectArg, mergeAggregates: Seq[AggFunction], sourceAggregates: Seq[AggFunction])
{
  def needsPostProcess = postprocess.expression match { 
    case Var(v) if v.equals(postprocess.name) => false
    case _ => true
  }
}

object DecomposeAggregates extends LazyLogging
{
  def decomposeAggregate(agg: AggFunction): Option[DecomposedAggregate] =
  {
    if(agg.distinct){ return None }
    agg.function match {
      case "SUM" | "GROUP_AND" | "GROUP_OR" | "FIRST" =>  
      {
        Some(DecomposedAggregate(
          ProjectArg(agg.alias, Var(agg.alias)),
          Seq(AggFunction(agg.function, false, Seq(Var(agg.alias)), agg.alias)),
          Seq(agg)
        ))
      }
      case "COUNT" => 
      {
        Some(DecomposedAggregate(
          ProjectArg(agg.alias, Var(agg.alias)),
          Seq(AggFunction("SUM", false, Seq(Var(agg.alias)), agg.alias)),
          Seq(agg)
        ))
      }
      case _ => 
      {
        logger.debug(s"Didn't decompose aggregate: ${agg.function}")
        None
      }
    }
  }

  def apply(o: Operator, typechecker: Typechecker): Operator =
  {
    o match {
      case Aggregate(gbCols, aggFns, src) => 
      {
        PullUpUnions.pullOutUnions(src) match {
          case Seq() => return EmptyTable(typechecker.schemaOf(o))
          case Seq(noUnions) => 
            // No unions... just a single sub node.  Return the aggregate as-is
            Aggregate(gbCols, aggFns, noUnions)
          case partitions =>
          {
            // There are some unions... see if we're allowed to decompose it
            val decompositions = 
              aggFns.map { decomposeAggregate(_) }
                    .map { 
                      case Some(s) => s 
                      case None => // we're not allowed to decompose it
                        return Aggregate(gbCols, aggFns, OperatorUtils.makeUnion(partitions))
                    }
            val aggregatedPartitions = 
              partitions.map { partition =>
                Aggregate(gbCols, 
                  decompositions.flatMap(_.sourceAggregates),
                  partition
                )
              }
            val mergedPartitions =
              Aggregate(gbCols, 
                decompositions.flatMap(_.mergeAggregates),
                OperatorUtils.makeUnion(aggregatedPartitions)
              )
            val replacement =
              if(decompositions.exists(_.needsPostProcess)){
                Project(
                  decompositions.map(_.postprocess),
                  mergedPartitions
                )
              } else { mergedPartitions }

            return replacement
          }
        }
      }
      case _ => 
        return o
    }
  }
}