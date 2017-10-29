package cyclon

import akka.actor._
import cyclon.LocalActor.{Neighbor, GetNeighbors}

import scala.concurrent.duration._
import scala.util.Random

class GossipActor(fanout: Int, cyclon: ActorRef) extends Actor {

  import GossipActor._
  import context.dispatcher

  var delivered: List[Message] = List[Message]()
  var pending: List[Pending] = List[Pending]()
  var neighs: List[Neighbor] = List[Neighbor]()

  val cancellable: Cancellable =
    context.system.scheduler.schedule(
      0 milliseconds,
      10000 milliseconds,
      self,
      AntiEntropy)

  override def receive: Unit = {

    case rBroadcast (m) =>
      val mess = new Message(m)
      val pend = new Pending(mess,self)
      //upper ! deliver
      delivered ::= mess
      pending ::= pend
      cyclon ! GetNeighbors

    case Neighbors(n) =>
      neighs = n
      for(p <- pending){
        val tmp = n.filter(!_.actor.pathString.equals(p.sender.path.toString))
        val gossipTargets = Random.shuffle(tmp).take(fanout)
        for(g <- gossipTargets)
          g.actor ! Receive("GossipMessage", p.m)
      }
      pending = List[Pending]()

    case Receive(typ, m) =>
      if(typ.equals("GossipMessage")){
        if(!delivered.contains(m)){
          delivered ::= m
          //upper ! deliver
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
  }
}

object GossipActor{

  class Message(var m: List[Neighbor] ) extends Serializable
  class Pending(val m: Message, val sender: ActorRef ) extends Serializable

  def props(fanout: Int, cyclon: ActorRef): Props =
    Props(new GossipActor(fanout, cyclon))

  case object AntiEntropy
  final case class rBroadcast(m:List[Neighbor])
  final case class Neighbors(n:List[Neighbor])
  final case class Receive(typ: String, m: Message)
  final case class ReceiveAnti(typ: String, d: List[Message])

}


