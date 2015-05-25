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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.swim.msg.IndirectPingAck;

import se.kth.swim.msg.Pong;
import se.kth.swim.msg.Status;
import se.kth.swim.msg.net.NetPing;
import se.kth.swim.msg.net.NetPong;
import se.kth.swim.msg.net.NetStatus;
import se.kth.swim.msg.net.NetIndirectPingRequest;
import se.kth.swim.msg.net.NetIndirectPingAck;
import se.kth.swim.msg.net.NetIndirectPing;
import se.kth.swim.msg.net.NetIndirectPong;
import se.kth.swim.msg.IndirectPong;
import se.kth.swim.msg.Ping;
import se.kth.swim.msg.net.NetParentChange;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.network.Network;
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
    private final Set<NatedAddress> bootstrapNodes;
    private final NatedAddress aggregatorAddress;

    private UUID pingTimeoutId;
    private UUID statusTimeoutId;

    private Map<NatedAddress, UUID> pongTimeoutMap = new HashMap<NatedAddress, UUID>();
    private Map<NatedAddress, UUID> suspectTimeoutMap = new HashMap<NatedAddress, UUID>();
    private Map<NatedAddress, UUID> indirectTimeoutMap = new HashMap<NatedAddress, UUID>();

    private int receivedPings = 0;
    private int incarnationNumber = 0;
    private Map<Peer, Integer> incarnationMap = new HashMap<Peer, Integer>();

    private int indirectPings = 1;
    private int lamda = 5;

    private Set<Peer> deadPeers = new HashSet<Peer>();
    private Set<Peer> alivePeers = new HashSet<Peer>();
    private Set<Peer> suspectedPeers = new HashSet<Peer>();
    private StateChanges<PeerStatus> queue = new StateChanges<PeerStatus>(4);

    public SwimComp(SwimInit init) {
        this.selfAddress = init.selfAddress;
//        log.info("{} initiating...", selfAddress);
        this.bootstrapNodes = init.bootstrapNodes;
        this.aggregatorAddress = init.aggregatorAddress;

        for (NatedAddress bootstrap : init.bootstrapNodes) {
            this.alivePeers.add(new Peer(bootstrap));
            this.incarnationMap.put(new Peer(bootstrap), 0);

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
//            log.info("{} stopping...", new Object[]{selfAddress.getId()});
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
            //log.info("{} received ping from:{}, sending Pong", new Object[]{selfAddress.getId(), event.getHeader().getSource().getId()});
            receivedPings++;
            if (!incarnationMap.containsKey(new Peer(event.getSource()))) {
                incarnationMap.put(new Peer(event.getSource()), 0);
                alivePeers.add(new Peer(event.getSource()));
                addToQueue(new Peer(event.getSource()), "alive", incarnationMap.get(new Peer(event.getSource())));
            } else if (deadPeers.remove(new Peer(event.getSource()))) {
                alivePeers.add(new Peer(event.getSource()));
                addToQueue(new Peer(event.getSource()), "alive", incarnationMap.get(new Peer(event.getSource())));
            }
            manageQueue(event.getContent().getQueue());
            cleanUpQueue();
            trigger(new NetPong(selfAddress, event.getSource(), new Pong(queue)), network);
        }
    };

    private Handler<NetPong> handlePong = new Handler<NetPong>() {

        @Override
        public void handle(NetPong event) {
            cancelPongTimeout(event.getSource());

            // if the peer responding to a Ping is suspected, remove it from the suspected list            
            if (suspectedPeers.remove(new Peer(event.getSource()))) {
                addToQueue(new Peer(event.getSource()), "alive", incarnationMap.get(new Peer(event.getSource())));
            }
            if (suspectTimeoutMap.containsKey(event.getSource())) {
                cancelSuspectTimeout(event.getSource());
            }
            manageQueue(event.getContent().getQueue());

        }
    };

    private Handler<NetParentChange> handleParentChange = new Handler<NetParentChange>() {

        @Override
        public void handle(NetParentChange event) {
            selfAddress = event.getSource();
            log.info("{} PARENT CHANGED", new Object[]{selfAddress.getParents()});
            addToQueue(new Peer(selfAddress), "parentChange", incarnationNumber);
        }
    };

    private Handler<PingTimeout> handlePingTimeout = new Handler<PingTimeout>() {

        @Override
        public void handle(PingTimeout event) {
            cleanUpQueue();
            for (Peer partnerAddress : alivePeers) {
                if (!partnerAddress.getPeer().isOpen()) {
                    log.info("{} SENDING PING TO {} WHICH HAS PARENTS {}", new Object[]{selfAddress.getId(), partnerAddress.getPeer().getId(), partnerAddress.getPeer().getParents()});
                }
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
            // send indirect ping
            log.info("{} PONG TIMEOUT, requesting indirect for  {}, pings: {}", new Object[]{selfAddress.getId(), event.getPeer(), receivedPings});
            log.info("{} PARENTS OF {} ARE {}", new Object[]{selfAddress.getId(), event.getPeer().getId(), event.getPeer().getParents()});
            scheduleIndirectTimeout(event.getPeer());
            int i = 0;
            for (Peer peer : alivePeers) {
                if (i++ < indirectPings) {
                    trigger(new NetIndirectPingRequest(selfAddress, peer.getPeer(), event.getPeer()), network);
                }
            }
        }
    };

    private Handler<NetIndirectPingRequest> handleIndirectPingRequest = new Handler<NetIndirectPingRequest>() {

        @Override
        public void handle(NetIndirectPingRequest event) {
//            log.info("{} got indirect request from {}", new Object[]{selfAddress, event.getSource()});
            trigger(new NetIndirectPing(selfAddress, event.getContent().getSuspected(), event.getSource()), network);
        }
    };

    private Handler<NetIndirectPing> handleIndirectPing = new Handler<NetIndirectPing>() {

        @Override
        public void handle(NetIndirectPing event) {
//            log.info("{} got indirect ping from {}", new Object[]{selfAddress, event.getSource()});
            IndirectPong reply = new IndirectPong(queue, event.getContent().getOriginal());
            trigger(new NetIndirectPong(selfAddress, event.getSource(), reply), network);
        }
    };

    private Handler<NetIndirectPong> handleIndirectPong = new Handler<NetIndirectPong>() {

        @Override
        public void handle(NetIndirectPong event) {
//            log.info("{} got indirect pong from {}", new Object[]{selfAddress, event.getSource()});
            IndirectPingAck ack = new IndirectPingAck(event.getContent().getQueue(), event.getSource());
            trigger(new NetIndirectPingAck(selfAddress, event.getContent().getOriginal(), ack), network);
        }
    };

    private Handler<NetIndirectPingAck> handleIndirectPingAck = new Handler<NetIndirectPingAck>() {

        @Override
        public void handle(NetIndirectPingAck event) {
//             log.info("{} got indirect ack from {}", new Object[]{selfAddress, event.getSource()});
            cancelIndirectTimeout(event.getContent().getFrom());
            manageQueue(event.getContent().getQueue());
        }
    };

    private Handler<IndirectTimeout> handleIndirectTimeout = new Handler<IndirectTimeout>() {

        @Override
        public void handle(IndirectTimeout event) {

            // suspect a Peer that didn't respond to a Ping and start a Timer to declare it dead after that time
            // if it is not suspected already
            if (!suspectedPeers.contains(new Peer(event.getPeer())) && !deadPeers.contains(new Peer(event.getPeer()))) {
                suspectedPeers.add(new Peer(event.getPeer()));
//                log.info("{} Indirect TIMEOUT, Suspecting peer {}, pings: {}", new Object[]{selfAddress.getId(), event.getPeer(), receivedPings});
                addToQueue(new Peer(event.getPeer()), "suspected", incarnationMap.get(new Peer(event.getPeer())));
                scheduleSuspectTimeout(event.getPeer());
            }
            indirectTimeoutMap.remove(event.getPeer());
        }
    };

    private Handler<DeclareDeadTimeout> handleSuspectTimeout = new Handler<DeclareDeadTimeout>() {

        @Override
        public void handle(DeclareDeadTimeout event) {
            // declare the peer dead
            log.info("{} Declaring peer {} Dead!", new Object[]{selfAddress.getId(), event.getPeer()});
            cancelPongTimeout(event.getPeer());
            deadPeers.add(new Peer(event.getPeer()));
            alivePeers.remove(new Peer(event.getPeer()));
            suspectedPeers.remove(new Peer(event.getPeer()));
            addToQueue(new Peer(event.getPeer()), "dead", incarnationMap.get(new Peer(event.getPeer())));
            suspectTimeoutMap.remove(event.getPeer());

        }
    };

    private Handler<StatusTimeout> handleStatusTimeout = new Handler<StatusTimeout>() {

        @Override
        public void handle(StatusTimeout event) {
//            log.info("{} sending status to aggregator:{}", new Object[]{selfAddress.getId(), aggregatorAddress});
            trigger(new NetStatus(selfAddress, aggregatorAddress, new Status(receivedPings, deadPeers.size(), alivePeers.size(), suspectedPeers.size())), network);
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
//        log.info("{} Canceling SUSPECT Timeout {}, timeoutID: {}", new Object[]{selfAddress.getId(), peer.getId(), pongTimeoutMap.get(peer)});
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
        CancelTimeout cpt = new CancelTimeout(pingTimeoutId);
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
        CancelTimeout cpt = new CancelTimeout(statusTimeoutId);
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

    public void addToQueue(Peer peer, String status, int number) {
        queue.remove(new PeerStatus(peer, "parentChange", number, 0));
        queue.remove(new PeerStatus(peer, "alive", number, 0));
        queue.remove(new PeerStatus(peer, "dead", number, 0));
        queue.remove(new PeerStatus(peer, "suspected", number, 0));
        queue.add(new PeerStatus(peer, status, number, 0));
    }

    public void cleanUpQueue() {
        Iterator<PeerStatus> iter = queue.iterator();
        while (iter.hasNext()) {
            PeerStatus state = iter.next();
            if (state.increaseLamdaCounter() > lamda) {
                iter.remove();
            }
        }
    }

    public void manageQueue(StateChanges<PeerStatus> receivedQueue) {
        for (PeerStatus address : receivedQueue) {
            if (!address.getPeer().getPeer().getId().equals(selfAddress.getId())) {
                if (!incarnationMap.containsKey(address.getPeer())) {
                    incarnationMap.put(address.getPeer(), 0);
                    alivePeers.add(address.getPeer());
                    addToQueue(address.getPeer(), "alive", 0);
                }
                if (address.getStatus().equals("suspected")
                        && !suspectedPeers.contains(address.getPeer())
                        && !deadPeers.contains(address.getPeer())) {

                    if (address.getIncarnationNumber() >= incarnationMap.get(address.getPeer())) {
                        addToQueue(address.getPeer(), "suspected", address.getIncarnationNumber());
                        incarnationMap.replace(address.getPeer(), address.getIncarnationNumber());
                        suspectedPeers.add(address.getPeer());
                        alivePeers.add(address.getPeer());
                    }
                } else if (address.getStatus().equals("alive")) {
                    if (address.getIncarnationNumber() > incarnationMap.get(address.getPeer())) {
                        if (suspectTimeoutMap.containsKey(address.getPeer().getPeer())) {
                            cancelSuspectTimeout(address.getPeer().getPeer());
                        }
                        addToQueue(address.getPeer(), "alive", address.getIncarnationNumber());
                        incarnationMap.replace(address.getPeer(), address.getIncarnationNumber());
                        suspectedPeers.remove(address.getPeer());
                        deadPeers.remove(address.getPeer());
                        alivePeers.add(address.getPeer());
                    }
                } else if (address.getStatus().equals("parentChange")) {
                    log.info("{} KNOWLEDGE ABOUT PARENTS OF {} RECEIVED: {}", new Object[]{selfAddress.getId(), address.getPeer().getPeer().getId(), address.getPeer().getPeer().getParents()});

                    if (checkIfParentsAreUpdated(address.getPeer())) {
                        log.info("{} CHANGED PARENTS OF {}", new Object[]{selfAddress.getId(), address.getPeer().getPeer().getId()});
                        addToQueue(address.getPeer(), "parentChange", address.getIncarnationNumber());
                    }
                } else if (address.getStatus().equals("dead") && !deadPeers.contains(address.getPeer())) {
                    addToQueue(address.getPeer(), "dead", address.getIncarnationNumber());
                    incarnationMap.replace(address.getPeer(), address.getIncarnationNumber());
                    deadPeers.add(address.getPeer());
                    alivePeers.remove(address.getPeer());
                    suspectedPeers.remove(address.getPeer());
                }
            } else {
                if (address.getStatus().equals("suspected") || address.getStatus().equals("dead")) {
                    //triger alive, send to all connections                        
                    if (address.getIncarnationNumber() == incarnationNumber) {
                        incarnationNumber++;
                        //    log.info("{} I am alive. Received word from {} that i was {}", new Object[]{receivedPings, event.getSource().getId(), address.getStatus()});
                        addToQueue(new Peer(selfAddress), "alive", incarnationNumber);
                    }
                }
            }
        }
    }

    public boolean checkIfParentsAreUpdated(Peer peerToCheck) {
        boolean checkDead;
        boolean checkAlive;
        boolean checkSuspected;
        checkDead = parentChangeToSet(peerToCheck, deadPeers);
        checkAlive = parentChangeToSet(peerToCheck, alivePeers);
        checkSuspected = parentChangeToSet(peerToCheck, suspectedPeers);

        return checkDead || checkAlive || checkSuspected;
    }

    public boolean parentChangeToSet(Peer peerToCheck, Set<Peer> set) {
        boolean check = false;

        if (set.contains(peerToCheck)) {
            Iterator<Peer> iter = set.iterator();
            while (iter.hasNext()) {
                Peer next = iter.next();
                if (next.getPeer().getId().equals(peerToCheck.getPeer().getId())) {
                    if (next.getPeer().getParents().size() != peerToCheck.getPeer().getParents().size()) {
                        iter.remove();

                        check = true;
                    } else {
                        if (!peerToCheck.getPeer().getParents().containsAll(next.getPeer().getParents())) {
                            check = true;
                        }
                    }
                }
            }
        }

        if (check) {
            set.add(peerToCheck);
        }
        return check;

    }
}
