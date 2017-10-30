package cyclon

import akka.actor._
import cyclon.GlobalActor.ReceiveGlobal
import cyclon.LocalActor.{GetNeighbors, Neighbor}
import cyclon.TestActor.TestDelivered

import scala.concurrent.duration._
import scala.util.Random

class GossipActor(fanout: Int, cyclon: ActorRef) extends Actor {

  import GossipActor._
  import context.dispatcher

  var delivered: List[Message] = List[Message]()
  var pending: List[Pending] = List[Pending]()
  var neighs: List[Neighbor] = List[Neighbor]()
  var global: ActorRef = null

  val cancellable: Cancellable =
    context.system.scheduler.schedule(
      0 milliseconds,
      5000 milliseconds,
      self,
      AntiEntropy)

  override def receive = {

    case rBroadcast () =>
      if(neighs.nonEmpty) {
        val myself = new Neighbor(context.actorSelection(self.path),0)
        val mess = new Message(myself :: neighs)
        val pend = new Pending(mess, self)
        global = sender()
        global ! ReceiveGlobal(neighs)
        delivered ::= mess
        pending ::= pend
      }
      cyclon ! GetNeighbors

    case Neighbors(n) =>
      neighs = n
      for(p <- pending){
        //val tmp = n.filter(!_.actor.pathString.equals(p.sender.path.toString))
        val gossipTargets = Random.shuffle(neighs).take(fanout)
        for(g <- gossipTargets)
          g.actor ! Receive("GossipMessage", p.m)
      }
      pending = List[Pending]()

    case Receive(typ, m) =>
      if(typ.equals("GossipMessage")){
        if(!delivered.contains(m)){
          delivered ::= m
          global ! ReceiveGlobal(m.m)
          pending ::= new Pending(m, sender())
          cyclon ! GetNeighbors
        }
      }

    case AntiEntropy =>
      if(neighs.nonEmpty){
        val n = Random.shuffle(neighs).take(1).head
        n.actor ! ReceiveAnti("AntiEntropyMsg", delivered)
      }

    case ReceiveAnti(typ, knownMessages) =>
      for(d <- delivered){
        if(!knownMessages.contains(d))
          sender() ! Receive("GossipMessage", d)
      }

    case GetDelivered =>
      sender() ! TestDelivered(delivered)
  }
}

object GossipActor{

  class Message(var m: List[Neighbor] ) extends Serializable
  class Pending(val m: Message, val sender: ActorRef ) extends Serializable

  def props(fanout: Int, cyclon: ActorRef): Props =
    Props(new GossipActor(fanout, cyclon))

  case object AntiEntropy
  case object GetDelivered
  final case class rBroadcast()
  final case class Neighbors(n:List[Neighbor])
  final case class Receive(typ: String, m: Message)
  final case class ReceiveAnti(typ: String, d: List[Message])

}


