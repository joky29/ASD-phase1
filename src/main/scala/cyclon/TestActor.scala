package cyclon

import akka.actor.{Actor, ActorRef, Props}
import cyclon.GlobalActor.GetGlobalView
import cyclon.GossipActor.{GetDelivered, Message}
import cyclon.LocalActor.{Neighbor, TestGetNeighborsCyclon}

class TestActor(cyclon: ActorRef, gossip: ActorRef, global: ActorRef) extends Actor{

  import TestActor._

  var globalView = List[Neighbor]()

  override def receive = {

    case TestGetNeighbors =>
      cyclon ! TestGetNeighborsCyclon

    case TestNeighbors(n) =>
      print("LocalNeighs - ")
      println(n.size)
      n.foreach(a => println(a.actor.pathString))
      println()

    case TestDelivered(delivered) =>
      print("Delivered - ")
      println(delivered.size)
      println()

    case TestGlobalView(global) =>
      print("GlobalView - ")
      println(global.size)
      global.foreach(g => println(g.actor.pathString))
      println()

    case message =>
      message match {
        case "L" => cyclon ! TestGetNeighborsCyclon
        case "G" => global ! GetGlobalView
        case "D" => gossip ! GetDelivered
      }
  }
}

object TestActor{

  def props(cyclon: ActorRef, gossip: ActorRef, global: ActorRef): Props =
    Props(new TestActor(cyclon, gossip, global))

  final case class TestNeighbors(n:List[Neighbor])
  case object TestGetNeighbors
  final case class TestGlobalView(global:List[Neighbor])
  final case class TestDelivered(delivered:List[Message])

}
