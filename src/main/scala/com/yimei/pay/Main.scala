package com.yimei.pay
import java.sql.Date

import slick.driver.H2Driver.api._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

import slick.backend.DatabasePublisher

/**
  * Created by wangqi on 16/11/6.
  */

case class Coffee (name:String,supID:Int,price:Double,sales:Int,total:Int,age:Date)
case class Supplier(id:Int,name:String,street:String,city:String,state:String,zip:String)
case class User(id:Option[Int],name:String)

object Main extends App{
  val db = Database.forConfig("h2mem1")
 // val db = Database.forURL("jdbc:h2:mem:hello", driver = "org.h2.Driver")

  // Definition of the SUPPLIERS table
  class Suppliers(tag: Tag) extends Table[Supplier](tag, "SUPPLIERS") {
    def id = column[Int]("SUP_ID", O.PrimaryKey) // This is the primary key column
    def name = column[String]("SUP_NAME")
    def street = column[String]("STREET")
    def city = column[String]("CITY")
    def state = column[String]("STATE")
    def zip = column[String]("ZIP")
    // Every table needs a * projection with the same type as the table's type parameter
    def * = (id, name, street, city, state, zip) <> (Supplier.tupled,Supplier.unapply)
  }
  val suppliers = TableQuery[Suppliers]

  // Definition of the COFFEES table
  class Coffees(tag: Tag) extends Table[Coffee](tag, "COFFEES") {
    def name = column[String]("COF_NAME", O.PrimaryKey)
    def supID = column[Int]("SUP_ID")
    def price = column[Double]("PRICE")
    def sales = column[Int]("SALES")
    def total = column[Int]("TOTAL")
    def age = column[Date]("Age")
    def * = (name, supID, price, sales, total,age) <> (Coffee.tupled,Coffee.unapply)
    // A reified foreign key relation that can be navigated to create a join
    def supplier = foreignKey("SUP_FK", supID, suppliers)(_.id)
  }
  val coffees = TableQuery[Coffees]

  class Users(tag: Tag) extends Table[User](tag, "users") {
    def id = column[Option[Int]]("id", O.PrimaryKey,O.AutoInc)
    def name = column[String]("name")
    def * = (id, name) <> (User.tupled,User.unapply)
  }
  val users = TableQuery[Users]

  val setup = DBIO.seq(
    // Create the tables, including primary and foreign keys
    (suppliers.schema ++ coffees.schema ++ users.schema).create,
    // Insert some suppliers
    suppliers += Supplier(101, "Acme, Inc.",      "99 Market Street", "Groundsville", "CA", "95199"),
    suppliers += Supplier( 49, "Superior Coffee", "1 Party Place",    "Mendocino",    "CA", "95460"),
    suppliers += Supplier(150, "The High Ground", "100 Coffee Lane",  "Meadows",      "CA", "93966"),
    // Equivalent SQL code:
    // insert into SUPPLIERS(SUP_ID, SUP_NAME, STREET, CITY, STATE, ZIP) values (?,?,?,?,?,?)

    // Insert some coffees (using JDBC's batch insert feature, if supported by the DB)
    coffees ++= Seq(
      Coffee("Colombian",         101, 7.99, 0, 0,new Date(1999,1,1)),
      Coffee("French_Roast",       49, 8.99, 0, 0,new Date(1999,1,1)),
      Coffee("Espresso",          150, 9.99, 0, 0,new Date(1999,1,1)),
      Coffee("Colombian_Decaf",   101, 8.99, 0, 0,new Date(1999,1,1)),
      Coffee("French_Roast_Decaf", 49, 9.99, 0, 0,new Date(1999,1,1))
    )
    // Equivalent SQL code:
    // insert into COFFEES(COF_NAME, SUP_ID, PRICE, SALES, TOTAL) values (?,?,?,?,?)
  )

  //(suppliers.schema ++ coffees.schema).create.statements.foreach(println)

  val setupFuture = db.run(setup)

  //如果不加这一句下面的Result有可能是空的.
  Await.ready(setupFuture,1000 millis)

  {
    println("Coffees:")
    val q = for (c <- coffees) yield c
    val a = q.result
    val f: Future[Seq[Coffee]] = db.run(a)

    f.onSuccess { case s => println(s"Result: $s") }


    Await.ready(f,1000 millis)
  }

  {
    val q = coffees.filter(_.price === 8.99).map(_.name).take(1)
    val a = q.result
    q.result.statements.foreach(println(_))
    val f: Future[Seq[String]] = db.run(a)
    f.onSuccess { case s => println(s"Result: $s") }


    Await.ready(f,1000 millis)
  }

  {
    val criteriaColombian = Option("Colombian")
    val criteriaEspresso = Option("Espresso")
    val criteriaRoast:Option[String] = None

    val q4 = coffees.filter { coffee =>
      List(
        criteriaColombian.map(coffee.name === _),
        criteriaEspresso.map(coffee.name === _),
        criteriaRoast.map(coffee.name === _) // not a condition as `criteriaRoast` evaluates to `None`
      ).collect({case Some(criteria)  => criteria}).reduceLeftOption(_ || _).getOrElse(true: Rep[Boolean])
    }
    val a = q4.result
    q4.result.statements.foreach(println(_))
    val f: Future[Seq[Coffee]] = db.run(a)
    f.onSuccess { case s => println(s"Result: $s") }


    Await.ready(f,1000 millis)
  }

  {
    val innerJoin = for {
      (c, s) <- coffees join suppliers on (_.supID === _.id)
    } yield (c, s)

    val a = innerJoin.result
    innerJoin.result.statements.foreach(println(_))
    val f: Future[Seq[(Coffee,Supplier)]] = db.run(a)
    f.onSuccess { case s => println(s"Result: $s") }


    Await.ready(f,1000 millis)

  }

  {
    val q = (for {
      c <- coffees
      s <- c.supplier
    } yield (c, s)).groupBy(_._1.supID)

    val q2 = q.map { case (supID, css) =>
      (supID, css.length, css.map(_._1.price).avg)
    }

    val a = q2.result
    q2.result.statements.foreach(println(_))
    val f: Future[Seq[(Int, Int, Option[Double])]] = db.run(a)
    f.onSuccess { case s => println(s"Result: $s") }


    Await.ready(f,1000 millis)

  }


  {
    val userId =
      (users returning users.map(_.id)) += User(None, "Stefan")
    val f: Future[Option[Int]] = db.run(userId)
    f onSuccess {
      case Some(l) => println(s"id = $l")
    }
    Await.ready(f,1000 millis)
  }

  {
    val q: Query[Coffees, Coffee, Seq] = for {c <- coffees if c.name === "Espresso" } yield c
    val updateAction = q.update(Coffee("wang",101, 7.99, 0, 0,new Date(1999,1,1)))
    val f: Future[Int] = db.run(updateAction)
    f onSuccess {
      case l => println(s"success insert $l records")
    }
    Await.ready(f,1000 millis)

    println("Coffees:")
    val q1: Query[Coffees, Coffee, Seq] = for (c <- coffees) yield c
    val a = q1.result
    val f1: Future[Seq[Coffee]] = db.run(a)

    f1.onSuccess { case s => println(s"Result: $s") }


    Await.ready(f1,1000 millis)

    // Get the statement without having to specify an updated value:
//    val sql = q.updateStatement
  }



}
