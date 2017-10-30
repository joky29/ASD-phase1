package cyclon

import akka.actor.{Actor, ActorRef, Props}
import cyclon.LocalActor.{Neighbor, TestGetNeighborsCyclon}

import scala.concurrent.duration._


class TestActor(cyclon: ActorRef) extends Actor{

  import TestActor._
  import context.dispatcher

  var globalView = List[Neighbor]()

  val cancellable =
    context.system.scheduler.schedule(
      0 milliseconds,
      5000 milliseconds,
      self,
      TestGetNeighbors)

  override def receive = {

    case TestGetNeighbors =>
      cyclon ! TestGetNeighborsCyclon

    case TestNeighbors(n) =>
      n.foreach(a => println("LocalNeighs",a.actor.pathString))
  }
}

object TestActor{

  def props(cyclon: ActorRef): Props =
    Props(new TestActor(cyclon))

  final case class TestNeighbors(n:List[Neighbor])
  case object TestGetNeighbors
  final case class ReceiveGlobal(view: List[Neighbor])

}
