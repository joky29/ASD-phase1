package cyclon

import akka.actor.{Actor, ActorRef, Props}
import cyclon.GlobalActor.Broadcast
import cyclon.LocalActor.{Neighbor}
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory

import scala.util.Random

class GlobalActor(gossip: ActorRef) extends Actor{

  import GlobalActor._
  import GossipActor._
  import context.dispatcher

  var globalView = List[Neighbor]()

  val cancellable =
    context.system.scheduler.schedule(
      0 milliseconds,
      5000 milliseconds,
      self,
      Broadcast)

  def contain (n: List[Neighbor], peer: Neighbor): Boolean ={
    n.foreach(n =>{
      if(n.actor.pathString.equals(peer.actor.pathString))
        return true
    })
    return false
  }

  def mergeViews(peerSample: List[Neighbor]): Unit ={
    for(peer <- peerSample) {
      if (!peer.actor.pathString.equals(self.path.toStringWithoutAddress)) {
        if(contain(globalView, peer)) {
          for (mine <- globalView) {
            if (peer.actor.pathString.equals(mine.actor.pathString) && peer.age < mine.age) {
              globalView = globalView.filter(!_.actor.pathString.equals(mine.actor.pathString))
              globalView ::= peer
            }
          }
        }
        else
          globalView ::= peer
      }
    }
  }

  override def receive = {
    case Broadcast =>
      gossip ! rBroadcast()

    case ReceiveGlobal(view) =>
      mergeViews(view)
      view.foreach(l => println("View",l.actor.pathString))
      println()
      globalView.foreach(l => println("GlobalNeighs",l.actor.pathString))
      println()
  }
}

object GlobalActor{

  def props(cyclon: ActorRef): Props =
    Props(new GlobalActor(cyclon))

  case object Broadcast
  final case class ReceiveGlobal(view: List[Neighbor])

}
