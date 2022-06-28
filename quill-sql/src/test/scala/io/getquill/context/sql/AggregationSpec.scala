package io.getquill.context.sql

import io.getquill.{ Literal, MirrorSqlDialect, Spec, SqlMirrorContext, TestEntities }

class AggregationSpec extends Spec {
  case class Person(id: Int, name: String, age: Int)
  case class PersonOpt(name: Option[String], age: Int)

  val ctx = new SqlMirrorContext(MirrorSqlDialect, Literal) with TestEntities
  import ctx._

  // an issue heavily related to the ability to use an aggregator and then a .nested after which
  // things like .filter can be called e.g:
  // .map(p => max(p.age)).nested.filter(n => ...)
  // is the fact that the max(n) will not be propagate forward out of the `nested` call.
  // That is to say, that we are guaranteed not to use normalizations that moves the max(p.age) aggregator
  // to the body of the outer query.
  // This action can be tested by a simpler operation .map(p => p.age + 1).nested.filter(p => 123)
  // If the +1 operation is propagate out, it will be moved to the outer query for example, the following
  // normalization should NOT happen:
  // SELECT p.age FROM (SELECT x.age + 1 FROM Person x) AS p WHERE p.age = 123
  //   => SELECT p.age + 1 FROM (SELECT x.age FROM Person x) AS p WHERE (p.age + 1) = 123
  "simple operation should not propogate from nested" in {
    ctx.run { query[Person].map(p => p.age + 1).nested.filter(p => p == 123) } mustEqual ""
    // should be: SELECT p.age FROM (SELECT x.age + 1 FROM Person x) AS p WHERE p.age = 123
  }

  "aggregation functions should" - {
    "work in a map clause that is last in a query" - {
      "max" in { ctx.run { query[Person].map(p => max(p.name)) } mustEqual "" }
      "min" in { ctx.run { query[Person].map(p => min(p.name)) } mustEqual "" }
      "avg" in { ctx.run { query[Person].map(p => avg(p.age)) } mustEqual "" }
      "sum" in { ctx.run { query[Person].map(p => sum(p.age)) } mustEqual "" }
    }

    "(strangely) work in a map clause that NOT last in a query - but is unused there" in {
      val q = quote {
        query[Person].map(p => (p.id, max(p.name))).filter(p => p._1 == "123")
      }
      ctx.run(q).string mustEqual ""
    }

    "works with optional mapping" in {
      val q = quote {
        query[PersonOpt].map(p => p.name.map(n => max(n)))
      }
      ctx.run(q).string mustEqual ""
    }
    "works with optional mapping + nested + filter" in {
      val q = quote {
        query[PersonOpt].map(p => p.name.map(n => max(n))).nested.filter(p => p == Option("Joe"))
      }
      ctx.run(q).string mustEqual ""
    }

    "works externally with optional mapping" - {
      "max" in { ctx.run { query[PersonOpt].map(p => max(p.name)) } mustEqual "" }
      "min" in { ctx.run { query[PersonOpt].map(p => min(p.name)) } mustEqual "" }
      "avg" in { ctx.run { query[PersonOpt].map(p => avg(p.age)) } mustEqual "" }
      "sum" in { ctx.run { query[PersonOpt].map(p => sum(p.age)) } mustEqual "" }
    }

    "fail in a map clause that is NOT last in a query" in {
      """ctx.run(query[Person].map(p => max(p.name)).filter(p => p == "Joe"))""" mustNot compile
      """ctx.run(query[Person].map(p => (p.name, max(p.name))).filter(n => n._1 == "Joe"))""" mustNot compile
    }
  }
}
