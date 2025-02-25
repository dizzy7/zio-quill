package io.getquill.context.jasync.mysql

import com.github.jasync.sql.db.{ QueryResult, ResultSetKt }
import io.getquill.ReturnAction.ReturnColumns

import scala.concurrent.ExecutionContext.Implicits.global
import io.getquill.{ Literal, MysqlJAsyncContext, ReturnAction, Spec }

class MysqlJAsyncContextSpec extends Spec {

  import testContext._

  "run non-batched action" in {
    val insert = quote { (i: Int) =>
      qr1.insert(_.i -> i)
    }
    await(testContext.run(insert(lift(1)))) mustEqual 1
  }

  "Insert with returning with single column table" in {
    val inserted: Long = await(testContext.run {
      qr4.insertValue(lift(TestEntity4(0))).returningGenerated(_.i)
    })
    await(testContext.run(qr4.filter(_.i == lift(inserted))))
      .head.i mustBe inserted
  }

  "performIO" in {
    await(performIO(runIO(qr4).transactional))
  }

  "probe" in {
    probe("select 1").toOption mustBe defined
  }

  "cannot extract" in {
    object ctx extends MysqlJAsyncContext(Literal, "testMysqlDB") {
      override def handleSingleResult[T](sql: String, list: List[T]) = super.handleSingleResult(sql, list)

      override def extractActionResult[O](
        returningAction:    ReturnAction,
        returningExtractor: ctx.Extractor[O]
      )(result: QueryResult) =
        super.extractActionResult(returningAction, returningExtractor)(result)
    }
    intercept[IllegalStateException] {
      val v = ctx.extractActionResult(ReturnColumns(List("w/e")), (row, session) => 1)(new QueryResult(0, "w/e", ResultSetKt.getEMPTY_RESULT_SET))
      ctx.handleSingleResult("<not used>", v)
    }
    ctx.close
  }

  "prepare" in {
    testContext.prepareParams("", (ps, session) => (Nil, ps ++ List("Sarah", 127))) mustEqual List("'Sarah'", "127")
  }

  override protected def beforeAll(): Unit = {
    await(testContext.run(qr1.delete))
    ()
  }
}
