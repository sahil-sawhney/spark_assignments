package com.knoldus

import org.scalatest._

class AssignmentSpec extends FlatSpec with Matchers {

  val assignment: Assignment = new Assignment("./src/test/resources/input.csv")

  "Assignment" should "have correct question 1" in {

    val result: Long = assignment.question1
    result should ===(32)
  }

  "Assignment" should "have correct question 2" in {

    val result: Array[Map[String, Any]] = assignment.question2

    result.length should ===(32)  //here 32 is the number of maps in the Array (else i had to create the array of maps on my own)
  }

  "Assignment" should "have correct question 3" in {

    val result: Long = assignment.question3
    result should ===(12)
  }

  "Assignment" should "have correct question 4" in {

    val result: Long = assignment.question4
    result should ===(5977)
  }

  "Assignment" should "have correct question 5" in {

    val result: Option[String] = assignment.question5
    result should ===(Some("Tenderloin"))
  }



}
