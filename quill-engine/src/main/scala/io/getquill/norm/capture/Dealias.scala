package io.getquill.norm.capture

import io.getquill.ast._
import io.getquill.norm.BetaReduction
import io.getquill.util.Interpolator
import io.getquill.util.Messages.TraceType

case class Dealias(state: Option[Ident]) extends StatefulTransformer[Option[Ident]] {

  val interp = new Interpolator(TraceType.Standard, 3)
  import interp._

  override def apply(q: Query): (Query, StatefulTransformer[Option[Ident]]) =
    q match {
      case FlatMap(a, b, c) =>
        dealias(a, b, c)(FlatMap) match {
          case (FlatMap(a, b, c), _) =>
            val (cn, cnt) = apply(c)
            (FlatMap(a, b, cn), cnt)
        }
      case ConcatMap(a, b, c) =>
        dealias(a, b, c)(ConcatMap) match {
          case (ConcatMap(a, b, c), _) =>
            val (cn, cnt) = apply(c)
            (ConcatMap(a, b, cn), cnt)
        }
      case Map(a, b, c) =>
        dealias(a, b, c)(Map)
      case Filter(a, b, c) =>
        dealias(a, b, c)(Filter)
      case SortBy(a, b, c, d) =>
        dealias(a, b, c)(SortBy(_, _, _, d))
      case GroupBy(a, b, c) =>
        dealias(a, b, c)(GroupBy)
      case GroupTo(qry, b, c, d, e) =>
        // First dealias the byIdent/byBody (b and c) based on the aliases in qry
        val (GroupTo(qry1, b1, c1, _, _), s1) = dealias(qry, b, c)(GroupTo(_, _, _, d, e))
        // Then use the result of that to dealias the toIdent/toBody (d and e)
        val (g2, s2) = new Dealias(s1.state).dealias(qry1, d, e)(GroupTo(_, b1, c1, _, _))
        println(s"=============== Dealias result: ${g2}")
        (g2, s2)
      case DistinctOn(a, b, c) =>
        dealias(a, b, c)(DistinctOn)
      case Take(a, b) =>
        val (an, ant) = apply(a)
        (Take(an, b), ant)
      case Drop(a, b) =>
        val (an, ant) = apply(a)
        (Drop(an, b), ant)
      case Union(a, b) =>
        val (an, _) = apply(a)
        val (bn, _) = apply(b)
        (Union(an, bn), Dealias(None))
      case UnionAll(a, b) =>
        val (an, _) = apply(a)
        val (bn, _) = apply(b)
        (UnionAll(an, bn), Dealias(None))
      case Join(t, a, b, iA, iB, o) =>
        val ((an, iAn, on), _) = dealias(a, iA, o)((_, _, _))
        val ((bn, iBn, onn), _) = dealias(b, iB, on)((_, _, _))
        (Join(t, an, bn, iAn, iBn, onn), Dealias(None))
      case FlatJoin(t, a, iA, o) =>
        val ((an, iAn, on), ont) = dealias(a, iA, o)((_, _, _))
        (FlatJoin(t, an, iAn, on), Dealias(Some(iA)))
      case _: Entity | _: Distinct | _: Aggregation | _: Nested =>
        (q, Dealias(None))
    }

  private def dealias[T](a: Ast, b: Ident, c: Ast)(f: (Ast, Ident, Ast) => T) =
    apply(a) match {
      case (an, t @ Dealias(Some(alias))) =>
        val retypedAlias = alias.copy(quat = b.quat)
        trace"Dealias $b into $retypedAlias".andLog()
        (f(an, retypedAlias, BetaReduction(c, b -> retypedAlias)), t)
      case other =>
        (f(a, b, c), Dealias(Some(b)))
    }
}

object Dealias {
  def apply(query: Query) =
    new Dealias(None)(query) match {
      case (q, _) => q
    }
}
