/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * GVoD is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.kth.swim;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.swim.msg.IndirectPingAck;
import se.kth.swim.msg.Pong;
import se.kth.swim.msg.net.NetPing;
import se.kth.swim.msg.net.NetPong;
import se.kth.swim.msg.net.NetIndirectPingRequest;
import se.kth.swim.msg.net.NetIndirectPingAck;
import se.kth.swim.msg.net.NetIndirectPing;
import se.kth.swim.msg.net.NetIndirectPong;
import se.kth.swim.msg.IndirectPong;
import se.kth.swim.msg.Ping;
import se.kth.swim.msg.Status;
import se.kth.swim.msg.net.NetParentChange;
import se.kth.swim.msg.net.NetStatus;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.CancelTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.p2ptoolbox.util.network.NatedAddress;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class SwimComp extends ComponentDefinition {

    private static final Logger log = LoggerFactory.getLogger(SwimComp.class);
    private Positive<Network> network = requires(Network.class);
    private Positive<Timer> timer = requires(Timer.class);

    private NatedAddress selfAddress;
    private final NatedAddress aggregatorAddress;

    private UUID pingTimeoutId;
    private UUID statusTimeoutId;

    private Map<NatedAddress, UUID> pongTimeoutMap = new HashMap<NatedAddress, UUID>();
    private Map<NatedAddress, UUID> suspectTimeoutMap = new HashMap<NatedAddress, UUID>();
    private Map<NatedAddress, UUID> indirectTimeoutMap = new HashMap<NatedAddress, UUID>();

    private int receivedPings = 0;
    private int incarnationNumber = 0;
    private int parentChanges = 0;
    private Map<Peer, Integer> incarnationMap = new HashMap<Peer, Integer>();
    private Map<Peer, Integer> parentChangesMap = new HashMap<Peer, Integer>();

    // Tweek this variables for different experiments.
    private int indirectPings = 1; // how many nodes are selected for indirect ping
    private final int lamda = 50; // how many times new information are passed around by the same node
    private StateChanges<PeerStatus> queue = new StateChanges<PeerStatus>(50); // the size of the piggyback

    private final Set<Peer> peersIHaveCommunicatedThisRound = new HashSet<Peer>();

    private Set<Peer> deadPeers = new HashSet<Peer>();
    private Set<Peer> alivePeers = new HashSet<Peer>();
    private Set<Peer> suspectedPeers = new HashSet<Peer>();

    public SwimComp(SwimInit init) {
        this.selfAddress = init.selfAddress;
        log.info("{} initiating...", selfAddress);
        this.aggregatorAddress = init.aggregatorAddress;

        // add the boostrap nodes to the alive set
        // set their incarnation number to 0
        for (NatedAddress bootstrap : init.bootstrapNodes) {
            this.alivePeers.add(new Peer(bootstrap));
            this.incarnationMap.put(new Peer(bootstrap), 0);
            this.parentChangesMap.put(new Peer(bootstrap), 0);
        }

        subscribe(handleStart, control);
        subscribe(handleStop, control);
        subscribe(handlePing, network);
        subscribe(handlePong, network);
        subscribe(handlePingTimeout, timer);
        subscribe(handlePongTimeout, timer);
        subscribe(handleSuspectTimeout, timer);
        subscribe(handleStatusTimeout, timer);
        subscribe(handleIndirectPingRequest, network);
        subscribe(handleIndirectPing, network);
        subscribe(handleIndirectPong, network);
        subscribe(handleIndirectPingAck, network);
        subscribe(handleIndirectTimeout, timer);
        subscribe(handleParentChange, network);

    }

    private Handler<Start> handleStart = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            log.info("{} starting... {}", new Object[]{selfAddress.getId(), selfAddress.getParents()});
            schedulePeriodicPing();
            schedulePeriodicStatus();
        }

    };

    private Handler<Stop> handleStop = new Handler<Stop>() {

        @Override
        public void handle(Stop event) {
            log.info("{} stopping...", new Object[]{selfAddress.getId()});
            if (pingTimeoutId != null) {
                cancelPeriodicPing();
            }
            if (statusTimeoutId != null) {
                cancelPeriodicStatus();
            }
        }

    };

    private Handler<NetPing> handlePing = new Handler<NetPing>() {

        @Override
        public void handle(NetPing event) {
//            log.info("{} received ping from:{}, sending Pong", new Object[]{selfAddress.getId(), event.getHeader().getSource().getId()});
            receivedPings++;
            // if we get a ping from a node we don't know existed
            // we add it to the alive set and set its incarnation number to 0
            if (!incarnationMap.containsKey(new Peer(event.getSource()))) {
                incarnationMap.put(new Peer(event.getSource()), 0);
                parentChangesMap.put(new Peer(event.getSource()), 0);

                alivePeers.add(new Peer(event.getSource()));
                addToQueue(new Peer(event.getSource()), "alive", incarnationMap.get(new Peer(event.getSource())), 0, 0);
            } else if (deadPeers.contains(new Peer(event.getSource())) || suspectedPeers.contains(new Peer(event.getSource()))) {
                // when we get a ping from a node that is presumed dead or suspected
                // we reply to the pong but we also tell him that we think he is dead to trigger 
                // the fightback mechanism with the incarnation number
                // we use the highest number for infection time, to not spread this information to other nodes as well
                addToQueue(new Peer(event.getSource()), "dead", incarnationMap.get(new Peer(event.getSource())), lamda - 1, parentChangesMap.get(new Peer(event.getSource())));
            }
            manageQueue(event.getContent().getQueue());
            cleanUpQueue();
            trigger(new NetPong(selfAddress, event.getSource(), new Pong(queue)), network);
        }
    };

    private Handler<NetPong> handlePong = new Handler<NetPong>() {

        @Override
        public void handle(NetPong event) {
            // cancel the pong timeout (to not trigger indirect)
            cancelPongTimeout(event.getSource());

//            log.info("{} received Pong from {}", new Object[]{selfAddress.getId(), event.getSource().getId()});
            // if the peer responding to a Ping is suspected, remove it from the suspected list            
            // cancel the suspect timeout, he replied 
            if (suspectTimeoutMap.containsKey(event.getSource())) {
                cancelSuspectTimeout(event.getSource());
            }
            manageQueue(event.getContent().getQueue());

        }
    };

    private Handler<NetParentChange> handleParentChange = new Handler<NetParentChange>() {

        @Override
        public void handle(NetParentChange event) {
            // NatTraversal detected dead parent
            // save the new address and dissiminate the information through swim
            selfAddress = event.getSource();
            parentChanges++;
//            log.info("{} Got new set of parents from NatTraversal {}", new Object[]{selfAddress.getId(), selfAddress.getParents()});
            addToQueue(new Peer(selfAddress), "parentChange", incarnationNumber, 0, parentChanges);
        }
    };

    private Handler<PingTimeout> handlePingTimeout = new Handler<PingTimeout>() {

        @Override
        public void handle(PingTimeout event) {
            cleanUpQueue();
            // send ping to a neighbor using Round Robin style
            Peer partnerAddress = roundRobinSelect();
            if (partnerAddress != null) {
                if (!partnerAddress.getPeer().getId().equals(selfAddress.getId())) {
                    trigger(new NetPing(selfAddress, partnerAddress.getPeer(), new Ping(queue)), network);
                    schedulePongTimeout(partnerAddress.getPeer());
                }
            }
        }
    };

    private Handler<PongTimeout> handlePongTimeout = new Handler<PongTimeout>() {

        @Override
        public void handle(PongTimeout event) {
//            log.info("{} Pong timeout, requesting indirect for  {}, pings: {}", new Object[]{selfAddress.getId(), event.getPeer(), receivedPings});
            // A peer didn't respond to Pong, request indirect timeout from another peer
            scheduleIndirectTimeout(event.getPeer());
            int i = 0;
            cleanUpQueue();
            for (Peer peer : alivePeers) {
                if (i < indirectPings && !peer.getPeer().equals(event.getPeer())) {
                    i++;
                    trigger(new NetIndirectPingRequest(selfAddress, peer.getPeer(), event.getPeer(), queue), network);
                }
            }
        }
    };

    private Handler<NetIndirectPingRequest> handleIndirectPingRequest = new Handler<NetIndirectPingRequest>() {

        @Override
        public void handle(NetIndirectPingRequest event) {
//            log.info("{} got indirect request from {}", new Object[]{selfAddress, event.getSource()});
            // Someone asked us to ping another node in its behalf
            trigger(new NetIndirectPing(selfAddress, event.getContent().getSuspected(), event.getSource(), event.getContent().getQueue()), network);
        }
    };

    private Handler<NetIndirectPing> handleIndirectPing = new Handler<NetIndirectPing>() {

        @Override
        public void handle(NetIndirectPing event) {
//            log.info("{} got indirect ping from {}", new Object[]{selfAddress, event.getSource()});
            // Received indirect ping, reply accordingly
            manageQueue(event.getContent().getQueue());
            IndirectPong reply = new IndirectPong(queue, event.getContent().getOriginal());
            trigger(new NetIndirectPong(selfAddress, event.getSource(), reply), network);
        }
    };

    private Handler<NetIndirectPong> handleIndirectPong = new Handler<NetIndirectPong>() {

        @Override
        public void handle(NetIndirectPong event) {
//            log.info("{} got indirect pong from {}", new Object[]{selfAddress, event.getSource()});
            // received the indirectPong, send ack to the ping requester
            IndirectPingAck ack = new IndirectPingAck(event.getContent().getQueue(), event.getSource());
            trigger(new NetIndirectPingAck(selfAddress, event.getContent().getOriginal(), ack), network);
        }
    };

    private Handler<NetIndirectPingAck> handleIndirectPingAck = new Handler<NetIndirectPingAck>() {

        @Override
        public void handle(NetIndirectPingAck event) {
//             log.info("{} got indirect ack from {}", new Object[]{selfAddress, event.getSource()});
            // received ack for indirect ping, just cancel the timeout and merge the piggyback
            cancelIndirectTimeout(event.getContent().getFrom());
            manageQueue(event.getContent().getQueue());
        }
    };

    private Handler<IndirectTimeout> handleIndirectTimeout = new Handler<IndirectTimeout>() {

        @Override
        public void handle(IndirectTimeout event) {
//                log.info("{} Indirect TIMEOUT, Suspecting peer {}, parents: {}", new Object[]{selfAddress.getId(), event.getPeer(), event.getPeer().getParents()});
            // suspect a Peer that didn't respond to a Ping and start a Timer to declare it dead after that time
            // if it is not suspected already
            if (!suspectedPeers.contains(new Peer(event.getPeer())) && !deadPeers.contains(new Peer(event.getPeer()))) {
                suspectedPeers.add(new Peer(event.getPeer()));
                addToQueue(new Peer(event.getPeer()), "suspected", incarnationMap.get(new Peer(event.getPeer())), 0, parentChangesMap.get(new Peer(event.getPeer())));
                scheduleSuspectTimeout(event.getPeer());
            }
            indirectTimeoutMap.remove(event.getPeer());
        }
    };

    private Handler<DeclareDeadTimeout> handleSuspectTimeout = new Handler<DeclareDeadTimeout>() {

        @Override
        public void handle(DeclareDeadTimeout event) {
            // declare the peer dead
//            log.info("{} Declaring peer {} Dead!", new Object[]{selfAddress.getId(), event.getPeer()});
            cancelPongTimeout(event.getPeer());
            deadPeers.add(new Peer(event.getPeer()));
            alivePeers.remove(new Peer(event.getPeer()));
            suspectedPeers.remove(new Peer(event.getPeer()));
            addToQueue(new Peer(event.getPeer()), "dead", incarnationMap.get(new Peer(event.getPeer())), 0, parentChangesMap.get(new Peer(event.getPeer())));
            suspectTimeoutMap.remove(event.getPeer());

        }
    };

    private Handler<StatusTimeout> handleStatusTimeout = new Handler<StatusTimeout>() {

        @Override
        public void handle(StatusTimeout event) {
//            log.info("{} STATUS:                              pings:{} d:{} a:{} s:{}", new Object[]{selfAddress.getId(), receivedPings, deadPeers.size(), alivePeers.size(), suspectedPeers.size()});
//            trigger(new NetStatus(selfAddress, aggregatorAddress, new Status(receivedPings, deadPeers, alivePeers, suspectedPeers)), network);
        }

    };

    private void scheduleIndirectTimeout(NatedAddress peer) {
        ScheduleTimeout spt = new ScheduleTimeout(4000);
        IndirectTimeout sc = new IndirectTimeout(spt, peer);
        spt.setTimeoutEvent(sc);
        indirectTimeoutMap.put(peer, sc.getTimeoutId());
        trigger(spt, timer);
    }

    private void cancelIndirectTimeout(NatedAddress peer) {
        CancelTimeout cpt = new CancelTimeout(indirectTimeoutMap.get(peer));
        indirectTimeoutMap.remove(peer);
        trigger(cpt, timer);
    }

    private void schedulePongTimeout(NatedAddress peer) {
        ScheduleTimeout spt = new ScheduleTimeout(2000);
        PongTimeout sc = new PongTimeout(spt, peer);
        spt.setTimeoutEvent(sc);
        pongTimeoutMap.put(peer, sc.getTimeoutId());
        trigger(spt, timer);
    }

    private void cancelPongTimeout(NatedAddress peer) {
        CancelTimeout cpt = new CancelTimeout(pongTimeoutMap.get(peer));
        pongTimeoutMap.remove(peer);
        trigger(cpt, timer);
    }

    private void scheduleSuspectTimeout(NatedAddress peer) {
        ScheduleTimeout spt = new ScheduleTimeout(15000);
        DeclareDeadTimeout sc = new DeclareDeadTimeout(spt, peer);
        spt.setTimeoutEvent(sc);
        suspectTimeoutMap.put(peer, sc.getTimeoutId());
        trigger(spt, timer);
    }

    private void cancelSuspectTimeout(NatedAddress peer) {
        CancelTimeout cpt = new CancelTimeout(suspectTimeoutMap.get(peer));
        suspectTimeoutMap.remove(peer);
        trigger(cpt, timer);
    }

    private void schedulePeriodicPing() {
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(5000, 5000);
        PingTimeout sc = new PingTimeout(spt);
        spt.setTimeoutEvent(sc);
        pingTimeoutId = sc.getTimeoutId();
        trigger(spt, timer);
    }

    private void cancelPeriodicPing() {
        CancelPeriodicTimeout cpt = new CancelPeriodicTimeout(pingTimeoutId);
        trigger(cpt, timer);
        pingTimeoutId = null;
    }

    private void schedulePeriodicStatus() {
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(10000, 10000);
        StatusTimeout sc = new StatusTimeout(spt);
        spt.setTimeoutEvent(sc);
        statusTimeoutId = sc.getTimeoutId();
        trigger(spt, timer);
    }

    private void cancelPeriodicStatus() {
        CancelPeriodicTimeout cpt = new CancelPeriodicTimeout(statusTimeoutId);
        trigger(cpt, timer);
        statusTimeoutId = null;
    }

    public static class SwimInit extends Init<SwimComp> {

        public final NatedAddress selfAddress;
        public final Set<NatedAddress> bootstrapNodes;
        public final NatedAddress aggregatorAddress;

        public SwimInit(NatedAddress selfAddress, Set<NatedAddress> bootstrapNodes, NatedAddress aggregatorAddress) {
            this.selfAddress = selfAddress;
            this.bootstrapNodes = bootstrapNodes;
            this.aggregatorAddress = aggregatorAddress;
        }
    }

    private static class StatusTimeout extends Timeout {

        public StatusTimeout(SchedulePeriodicTimeout request) {
            super(request);
        }
    }

    private static class PingTimeout extends Timeout {

        public PingTimeout(SchedulePeriodicTimeout request) {
            super(request);
        }
    }

    private static class PongTimeout extends Timeout {

        private final NatedAddress peer;

        public PongTimeout(ScheduleTimeout request, NatedAddress peer) {
            super(request);
            this.peer = peer;
        }

        public NatedAddress getPeer() {
            return peer;
        }

    }

    private static class DeclareDeadTimeout extends Timeout {

        private final NatedAddress peer;

        public DeclareDeadTimeout(ScheduleTimeout request, NatedAddress peer) {
            super(request);
            this.peer = peer;
        }

        public NatedAddress getPeer() {
            return peer;
        }

    }

    private static class IndirectTimeout extends Timeout {

        private final NatedAddress peer;

        public IndirectTimeout(ScheduleTimeout request, NatedAddress peer) {
            super(request);
            this.peer = peer;
        }

        public NatedAddress getPeer() {
            return peer;
        }

    }

    // when we add a new state change to the piggyback, we remove any old information regarding that node
    public void addToQueue(Peer peer, String status, int number, int lamda, int parentNumber) {
        queue.remove(new PeerStatus(peer, "parentChange", number, lamda, parentNumber));
        queue.remove(new PeerStatus(peer, "alive", number, lamda, parentNumber));
        queue.remove(new PeerStatus(peer, "dead", number, lamda, parentNumber));
        queue.remove(new PeerStatus(peer, "suspected", number, lamda, parentNumber));
        sortQueueAccordingToInfectionTime();
        queue.add(new PeerStatus(peer, status, number, lamda, parentNumber));
        trigger(new NetStatus(selfAddress, aggregatorAddress, new Status(receivedPings, deadPeers, alivePeers, suspectedPeers)), network);

    }

    private void sortQueueAccordingToInfectionTime() {
        Collections.sort(queue, new Comparator<PeerStatus>() {

            public int compare(PeerStatus o1, PeerStatus o2) {
                if (o1.getLamdaCounter() > o2.getLamdaCounter()) {
                    return -1;
                } else if (o1.getLamdaCounter() < o2.getLamdaCounter()) {
                    return 1;
                } else {
                    return 0;
                }
            }
        });
    }

    // remove items from the piggyback that we already sent infection_time times
    public void cleanUpQueue() {
        Iterator<PeerStatus> iter = queue.iterator();
        while (iter.hasNext()) {
            PeerStatus state = iter.next();
            if (state.increaseLamdaCounter() > lamda) {
                iter.remove();
            }
        }
    }

    // merge received queue with local state
    public void manageQueue(StateChanges<PeerStatus> receivedQueue) {
        for (PeerStatus address : receivedQueue) {
            if (!address.getPeer().getPeer().getId().equals(selfAddress.getId())) {
                if (!incarnationMap.containsKey(address.getPeer())) {
                    incarnationMap.put(address.getPeer(), 0);
                    alivePeers.add(address.getPeer());
                    parentChangesMap.put(address.getPeer(), 0);
                    addToQueue(address.getPeer(), "alive", 0, 0, 0);
                }
                if (address.getStatus().equals("suspected")
                        && !deadPeers.contains(address.getPeer())
                        && !suspectedPeers.contains(address.getPeer())
                        && address.getIncarnationNumber() >= incarnationMap.get(address.getPeer())) {
                    addToQueue(address.getPeer(), "suspected", address.getIncarnationNumber(), 0, address.getNumberOfParentChanges());
                    incarnationMap.replace(address.getPeer(), address.getIncarnationNumber());
                    parentChangesMap.replace(address.getPeer(), address.getNumberOfParentChanges());
                    suspectedPeers.add(address.getPeer());
                    alivePeers.add(address.getPeer());

                } else if (address.getStatus().equals("suspected")
                        && !deadPeers.contains(address.getPeer())
                        && suspectedPeers.contains(address.getPeer())
                        && address.getIncarnationNumber() > incarnationMap.get(address.getPeer())) {
                    addToQueue(address.getPeer(), "suspected", address.getIncarnationNumber(), 0, address.getNumberOfParentChanges());
                    incarnationMap.replace(address.getPeer(), address.getIncarnationNumber());
                    parentChangesMap.replace(address.getPeer(), address.getNumberOfParentChanges());

                } else if (address.getStatus().equals("alive")
                        && address.getIncarnationNumber() > incarnationMap.get(address.getPeer())) {
                    if (suspectTimeoutMap.containsKey(address.getPeer().getPeer())) {
                        cancelSuspectTimeout(address.getPeer().getPeer());
                    }
                    addToQueue(address.getPeer(), "alive", address.getIncarnationNumber(), 0, address.getNumberOfParentChanges());
                    incarnationMap.replace(address.getPeer(), address.getIncarnationNumber());
                    parentChangesMap.replace(address.getPeer(), address.getNumberOfParentChanges());
                    suspectedPeers.remove(new Peer(address.getPeer().getPeer()));
                    deadPeers.remove(address.getPeer());
                    alivePeers.add(address.getPeer());
                } else if (address.getStatus().equals("parentChange")
                        && address.getNumberOfParentChanges() > parentChangesMap.get(address.getPeer())) {
                    if (checkIfParentsAreUpdated(address.getPeer())) {
//                            log.info("{} Knowledge about parents of {} received: {}", new Object[]{selfAddress.getId(), address.getPeer().getPeer().getId(), address.getPeer().getPeer().getParents()});
                        addToQueue(address.getPeer(), "parentChange", address.getIncarnationNumber(), 0, address.getNumberOfParentChanges());
//                        incarnationMap.replace(address.getPeer(), address.getIncarnationNumber());
                        parentChangesMap.replace(address.getPeer(), address.getNumberOfParentChanges());
                    }
                } else if (address.getStatus().equals("dead") && !deadPeers.contains(address.getPeer())) {
                    if (address.getIncarnationNumber() >= incarnationMap.get(address.getPeer())) {
                        addToQueue(address.getPeer(), "dead", address.getIncarnationNumber(), 0, address.getNumberOfParentChanges());

                        incarnationMap.replace(address.getPeer(), address.getIncarnationNumber());
                        parentChangesMap.replace(address.getPeer(), address.getNumberOfParentChanges());
                        deadPeers.add(address.getPeer());
                        alivePeers.remove(address.getPeer());
                        suspectedPeers.remove(address.getPeer());
                    }

                }
            } else {
                if (address.getStatus().equals("suspected") || address.getStatus().equals("dead")) {
                    log.info("{} I am alive. Received word that i was {} inc:{} myinc:{}", new Object[]{selfAddress.getId(), address.getStatus(), address.getIncarnationNumber(), incarnationNumber});
                    if (address.getIncarnationNumber() == incarnationNumber) {
                        incarnationNumber++;
                        addToQueue(new Peer(selfAddress), "alive", incarnationNumber, 0, parentChanges);
                        log.info("{} added to queue that I am alive", new Object[]{selfAddress.getId()});
                    }
                }
            }
        }
    }

    // this functions checks if we have already merged the parent change of a node
    public boolean checkIfParentsAreUpdated(Peer peerToCheck) {
        boolean checkDead;
        boolean checkAlive;
        boolean checkSuspected;
        checkDead = parentChangeToSet(peerToCheck, deadPeers);
        checkAlive = parentChangeToSet(peerToCheck, alivePeers);
        checkSuspected = parentChangeToSet(peerToCheck, suspectedPeers);

        return checkDead || checkAlive || checkSuspected;
    }

    // returns true when the set doesn't contain the peer with the new parents
    public boolean parentChangeToSet(Peer peerToCheck, Set<Peer> set) {
        boolean check = false;
        NatedAddress oldAddress = null;
        if (set.contains(peerToCheck)) {
            Iterator<Peer> iter = set.iterator();
            while (iter.hasNext()) {
                Peer next = iter.next();
                if (next.getPeer().getId().equals(peerToCheck.getPeer().getId())) {
                    if (next.getPeer().getParents().size() != peerToCheck.getPeer().getParents().size()) {
                        oldAddress = next.getPeer();
                        iter.remove();
                        check = true;
                    } else {
                        if (!peerToCheck.getPeer().getParents().containsAll(next.getPeer().getParents())) {
                            oldAddress = next.getPeer();
                            iter.remove();
                            check = true;
                        }
                    }
                }
            }
        }
        if (check) {
            cancelPongTimeout(oldAddress);
            cancelSuspectTimeout(oldAddress);
            cancelIndirectTimeout(oldAddress);
            set.add(peerToCheck);
        }
        return check;

    }

    // select the a peer from alivePeers that we haven't communicated with yet this round
    private Peer roundRobinSelect() {
        if (alivePeers.size() > 0) {
            Iterator<Peer> it = alivePeers.iterator();
            while (it.hasNext()) {
                Peer toReturn = it.next();
                if (!peersIHaveCommunicatedThisRound.contains(toReturn)) {
                    peersIHaveCommunicatedThisRound.add(toReturn);
                    return toReturn;
                }
            }
            // if there isn't any left we shuffle the set and return the first
            return shuffleAliveSet();
        }
        return null;
    }

    // shuffles the set and returns the first entry
    private Peer shuffleAliveSet() {
        peersIHaveCommunicatedThisRound.clear();
        List<Peer> alivePeersToShuffle = new ArrayList<Peer>(alivePeers);
        Collections.shuffle(alivePeersToShuffle);
        alivePeers = new HashSet<Peer>(alivePeersToShuffle);
        peersIHaveCommunicatedThisRound.add(alivePeersToShuffle.get(0));
        return alivePeersToShuffle.get(0);
    }
}
