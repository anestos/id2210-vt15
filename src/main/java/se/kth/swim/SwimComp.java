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
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.kth.swim.msg.Pong;
import se.kth.swim.msg.Status;
import se.kth.swim.msg.net.NetPing;
import se.kth.swim.msg.net.NetPong;
import se.kth.swim.msg.net.NetStatus;
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

    private final NatedAddress selfAddress;
    private final Set<NatedAddress> bootstrapNodes;
    private final NatedAddress aggregatorAddress;

    private UUID pingTimeoutId;
    private UUID statusTimeoutId;

    private Map<NatedAddress, UUID> pongTimeoutMap = new HashMap<NatedAddress, UUID>();
    private Map<NatedAddress, UUID> suspectTimeoutMap = new HashMap<NatedAddress, UUID>();

    private int receivedPings = 0;

    private Set<Peer> deadPeers = new HashSet<Peer>();
    private Set<Peer> alivePeers = new HashSet<Peer>();
    private Set<Peer> suspectedPeers = new HashSet<Peer>();

    public SwimComp(SwimInit init) {
        this.selfAddress = init.selfAddress;
        log.info("{} initiating...", selfAddress);
        this.bootstrapNodes = init.bootstrapNodes;
        this.aggregatorAddress = init.aggregatorAddress;

        for (NatedAddress bootstrap : init.bootstrapNodes) {
            this.alivePeers.add(new Peer(bootstrap));
        }

        subscribe(handleStart, control);
        subscribe(handleStop, control);
        subscribe(handlePing, network);
        subscribe(handlePong, network);
        subscribe(handlePingTimeout, timer);
        subscribe(handlePongTimeout, timer);
        subscribe(handleSuspectTimeout, timer);
        subscribe(handleStatusTimeout, timer);
    }

    private Handler<Start> handleStart = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            log.info("{} starting...", new Object[]{selfAddress.getId()});
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
            log.info("{} received ping from:{}, sending Pong", new Object[]{selfAddress.getId(), event.getHeader().getSource().getId()});
            receivedPings++;
            alivePeers.add(new Peer(event.getSource()));
            trigger(new NetPong(selfAddress, event.getSource(), new Pong(deadPeers, suspectedPeers, alivePeers)), network);
        }

    };

    private Handler<NetPong> handlePong = new Handler<NetPong>() {

        @Override
        public void handle(NetPong event) {
            cancelPongTimeout(event.getSource());
            log.info("{} received pong from:{}", new Object[]{selfAddress.getId(), event.getHeader().getSource()});
            
            // if the peer responding to a Ping is suspected, remove it from the suspected list
            if (suspectedPeers.remove(new Peer(event.getSource()))){
                cancelSuspectTimeout(event.getSource());
                log.info("removing peer {} from suspected list", new Object[]{event.getSource().getId()});
            }
            for (Peer address : event.getContent().getSuspected()) {
                suspectedPeers.add(address);
            }
            for (Peer address : event.getContent().getDead()) {
                deadPeers.add(address);
            }
            for (Peer address : event.getContent().getAlive()) {
                alivePeers.add(address);
            }
        }

    };

    private Handler<PingTimeout> handlePingTimeout = new Handler<PingTimeout>() {

        @Override
        public void handle(PingTimeout event) {

            for (Peer partnerAddress : alivePeers) {
                if (partnerAddress.getPeer().getId() != selfAddress.getId()) {
                    log.info("{} sending ping to partner:{}", new Object[]{selfAddress.getId(), partnerAddress.getPeer().getId()});
                    trigger(new NetPing(selfAddress, partnerAddress.getPeer()), network);
                    schedulePongTimeout(partnerAddress.getPeer());
                }
            }
        }
    };

    private Handler<PongTimeout> handlePongTimeout = new Handler<PongTimeout>() {

        @Override
        public void handle(PongTimeout event) {
            // suspect a Peer that didn't respond to a Ping and start a Timer to declare it dead after that time
            // if it is not suspected already
            if (!suspectedPeers.contains(new Peer(event.getPeer()))){
                log.info("{} PONG TIMEOUT, Suspecting peer {}", new Object[]{selfAddress.getId(), event.getPeer()});
                suspectedPeers.add(new Peer(event.getPeer()));
                scheduleSuspectTimeout(event.getPeer());
            }
                pongTimeoutMap.remove(event.getPeer());
            
        }
    };

    private Handler<DeclareDeadTimeout> handleSuspectTimeout = new Handler<DeclareDeadTimeout>() {

        @Override
        public void handle(DeclareDeadTimeout event) {
            // If the peer is still on the suspected list, declare it dead
            if (suspectedPeers.contains(new Peer(event.getPeer()))){
                log.info("{} Declaring peer {} Dead!", new Object[]{selfAddress.getId(), event.getPeer()});
                deadPeers.add(new Peer(event.getPeer()));
                alivePeers.remove(new Peer(event.getPeer()));
                suspectedPeers.remove(new Peer(event.getPeer()));
            }
            suspectTimeoutMap.remove(event.getPeer());

        }
    };

    private Handler<StatusTimeout> handleStatusTimeout = new Handler<StatusTimeout>() {

        @Override
        public void handle(StatusTimeout event) {
            log.info("{} sending status to aggregator:{}", new Object[]{selfAddress.getId(), aggregatorAddress});
            trigger(new NetStatus(selfAddress, aggregatorAddress, new Status(receivedPings)), network);
        }

    };

    private void schedulePongTimeout(NatedAddress peer) {
        ScheduleTimeout spt = new ScheduleTimeout(500);
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
        ScheduleTimeout spt = new ScheduleTimeout(5000);
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
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(1000, 1000);
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

        private NatedAddress peer;

        public PongTimeout(ScheduleTimeout request, NatedAddress peer) {
            super(request);
            this.peer = peer;
        }

        public NatedAddress getPeer() {
            return peer;
        }

    }

    private static class DeclareDeadTimeout extends Timeout {

        private NatedAddress peer;

        public DeclareDeadTimeout(ScheduleTimeout request, NatedAddress peer) {
            super(request);
            this.peer = peer;
        }

        public NatedAddress getPeer() {
            return peer;
        }

    }
}
