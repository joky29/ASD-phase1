//#full-example
package com.lightbend.akka.sample

import org.scalatest.{ BeforeAndAfterAll, FlatSpecLike, Matchers }
import akka.actor.{ Actor, Props, ActorSystem }
import akka.testkit.{ ImplicitSender, TestKit, TestActorRef, TestProbe }
import scala.concurrent.duration._

//#test-classes
class AkkaQuickstartSpec(_system: ActorSystem)
  extends TestKit(_system)
  with Matchers
  with FlatSpecLike
  with BeforeAndAfterAll {
  //#test-classes

  def this() = this(ActorSystem("AkkaQuickstartSpec"))

  override def afterAll: Unit = {
    shutdown(system)
  }

  //#first-test
  //#specification-example
  "A Greeter Actor" should "pass on a greeting message when instructed to" in {
    //#specification-example
    val testProbe = TestProbe()
    val helloGreetingMessage = "hello"

  }
  //#first-test
}
//#full-example
