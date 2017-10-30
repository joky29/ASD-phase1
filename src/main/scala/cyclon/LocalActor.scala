package cyclon

import java.io.File

import akka.actor.{Actor, ActorSelection, ActorSystem, ExtendedActorSystem, Props}
import com.typesafe.config.ConfigFactory
import cyclon.GossipActor.Neighbors
import cyclon.TestActor.TestNeighbors
//import cyclon.GossipActor.Neighbors

import scala.concurrent.duration._
import scala.util.Random
import scala.io.StdIn.readLine

class LocalActor(ip:String, port: String, name: String, maxN: Int) extends Actor{

  import LocalActor._
  import context.dispatcher

  var neighs = List[Neighbor]()
  var sample = List[Neighbor]()
  val N = maxN
  val cancellable =
    context.system.scheduler.schedule(
      0 milliseconds,
      10000 milliseconds,
      self,
      Shuffle())

  override def preStart(): Unit = {
    if(!port.equals("")) {
      println("AQUIIII")
      val contact = context.system.actorSelection("akka.tcp://CyclonSystem@" + ip + ":" + port + "/user/" + name)
      println("That 's remote:" + contact)
      val contactF = new Neighbor(contact, 0)
      neighs = contactF :: neighs
    }
  }

  def contain (n: List[Neighbor], peer: Neighbor): Boolean ={
    n.foreach(n =>{
      if(n.actor.pathString.equals(peer.actor.pathString))
        return true
    })
    return false
  }

  def mergeViews(peerSample: List[Neighbor], mySample: List[Neighbor]): Unit ={
    for(peer <- peerSample) {
      if (!peer.actor.pathString.equals(self.path.toStringWithoutAddress)) {
        if(contain(neighs, peer)) {
          for (mine <- neighs) {
            if (peer.actor.pathString.equals(mine.actor.pathString) && peer.age < mine.age) {
              neighs = neighs.filter(!_.actor.pathString.equals(mine.actor.pathString))
              neighs ::= peer
            }
          }
        }
        else if (neighs.length < maxN) {
          neighs ::= peer
        }
        else {
          var x: Neighbor = null
          if (neighs.containsSlice(peerSample)) {
            var done = false
            for (n <- neighs; if !done) {
              if (mySample.contains(n)) {
                x = n
                done = true
              }
            }
          }
          else {
            x = Random.shuffle(neighs).take(1).head
          }
          if(!neighs.contains(peer)) {
            neighs = neighs.filter(!_.actor.pathString.equals(x.actor.pathString))
            neighs ::= peer
          }
        }
      }
    }
  }

  override def receive = {

    case GetNeighbors =>
      sender() ! Neighbors( neighs )

    case TestGetNeighborsCyclon =>
      sender() ! TestNeighbors( neighs )

    case Shuffle() =>
      if(!neighs.isEmpty) {
        var oldest:Neighbor = new Neighbor(null, -1000)
        for (neigh <- neighs){
          neigh.age += 1
          if(oldest.age < neigh.age)
            oldest = neigh
        }
        //neighs = neighs.filter(!_.actor.pathString.equals(oldest.actor.pathString))
        sample = Random.shuffle(neighs).take(2)
        val myself = new Neighbor(context.actorSelection(self.path),0)

        oldest.actor ! Receive("shuffleRequest", myself :: sample)
      }

    case Receive(request: String, peerSample: List[Neighbor]) =>
      if(request.equals("shuffleRequest")){
        if(!neighs.isEmpty) {
          val tempSample = Random.shuffle(neighs).take(2)
          sender() ! Receive("shuffleReply", tempSample)
          mergeViews(peerSample, tempSample)
        }
        else
          mergeViews(peerSample, sample)
      }
      else if(request.equals("shuffleReply")){
        mergeViews(peerSample, sample)
      }
  }
}


object LocalActor {

  class Neighbor(val actor: ActorSelection, var age: Int ) extends Serializable

  final case class Shuffle()
  case object GetNeighbors
  case object TestGetNeighborsCyclon
  final case class Receive(request: String, peerSample: List[Neighbor])

  def props(ip:String, port: String, name: String, maxN: Int): Props =
    Props(new LocalActor(ip,port,name, maxN))

  def main(args: Array[String]) {

    println("Indicar vizinho conhecido")
    //val ip = readLine("IP: ")
    val port = readLine("Porta: ")
    //val name = readLine("Nome: ")

    val configFile = getClass.getClassLoader.getResource("local_application.conf").getFile
    val config = ConfigFactory.parseFile(new File(configFile))
    val system = ActorSystem("CyclonSystem",config)
    val myname = "Actor" + system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress.port.get


    val cyclonActor = system.actorOf(LocalActor.props("127.0.0.1", port, "Actor"+port , 3), myname)
    val gossipActor = system.actorOf(GossipActor.props(fanout = 4, cyclonActor))
    val globalActor = system.actorOf(GlobalActor.props(gossipActor))
    val testActor = system.actorOf(TestActor.props(cyclonActor))
  }


}
