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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.swim.croupier.CroupierPort;
import se.kth.swim.croupier.msg.CroupierSample;
import se.kth.swim.croupier.util.Container;
import se.kth.swim.msg.net.NetHeartbeatReply;
import se.kth.swim.msg.net.NetHeartbeat;
import se.kth.swim.msg.net.NetMsg;
import se.kth.swim.msg.net.NetParentChange;
import se.kth.swim.msg.net.NetParentRequest;
import se.kth.swim.msg.net.NetParentRequestAck;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.network.Header;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.CancelTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.p2ptoolbox.util.network.NatType;
import se.sics.p2ptoolbox.util.network.NatedAddress;
import se.sics.p2ptoolbox.util.network.impl.BasicAddress;
import se.sics.p2ptoolbox.util.network.impl.BasicNatedAddress;
import se.sics.p2ptoolbox.util.network.impl.RelayHeader;
import se.sics.p2ptoolbox.util.network.impl.SourceHeader;

/**
 *
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class NatTraversalComp extends ComponentDefinition {

    private static final Logger log = LoggerFactory.getLogger(NatTraversalComp.class);
    private Negative<Network> local = provides(Network.class);
    private Positive<Network> network = requires(Network.class);
    private Positive<CroupierPort> croupier = requires(CroupierPort.class);
    private Positive<Timer> timer = requires(Timer.class);

    private NatedAddress selfAddress;
    private final Random rand;
    private UUID heartbeatTimeoutId;
    private Map<NatedAddress, UUID> heartbeatTimeoutMap = new HashMap<NatedAddress, UUID>();
    private boolean foundDeadParent = false;
    private boolean changingParentsNow = false;
    public Set<Container<NatedAddress, Object>> publicSample = new HashSet<Container<NatedAddress, Object>>();

    private final int maximumParents = 3;
    private Set<NatedAddress> newParents = new HashSet<NatedAddress>();
    private final int myid;
    private static InetAddress localHost;

    static {
        try {
            localHost = InetAddress.getByName("127.0.0.1");
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        }
    }

    public NatTraversalComp(NatTraversalInit init) {
        this.selfAddress = init.selfAddress;
        log.info("{} {} initiating...", new Object[]{selfAddress.getId(), (selfAddress.isOpen() ? "OPEN" : "NATED")});
        this.myid = selfAddress.getId();

        this.rand = new Random(init.seed);
        subscribe(handleStart, control);
        subscribe(handleStop, control);
        subscribe(handleIncomingMsg, network);
        subscribe(handleHeartbeatReply, network);
        subscribe(handleHeartbeat, network);
        subscribe(handleParentRequest, network);
        subscribe(handleParentRequestAck, network);
        subscribe(handleOutgoingMsg, local);
        subscribe(handleHeartbeatTimeout, timer);
        subscribe(handleHeartbeatReplyTimeout, timer);
        subscribe(handleParentChangeTimeout, timer);
        subscribe(handleCroupierSample, croupier);
    }

    private Handler<Start> handleStart = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            log.info("{} starting...", new Object[]{selfAddress.getId()});
            // start a periodic timeout to heartbeat the parents if the node is Nated
            if (!selfAddress.isOpen()) {
                schedulePeriodicHeartbeat();
            }
        }

    };
    private Handler<Stop> handleStop = new Handler<Stop>() {

        @Override
        public void handle(Stop event) {
            log.info("{} stopping...", new Object[]{selfAddress.getId()});
            cancelPeriodicHeartbeat();
        }

    };

    private Handler<NetMsg<Object>> handleIncomingMsg = new Handler<NetMsg<Object>>() {

        @Override
        public void handle(NetMsg<Object> msg) {
            log.trace("{} received msg:{}", new Object[]{selfAddress.getId(), msg});
            Header<NatedAddress> header = msg.getHeader();
            if (header instanceof SourceHeader) {
                if (!selfAddress.isOpen()) {
                    throw new RuntimeException("source header msg received on nated node - nat traversal logic error");
                }
                SourceHeader<NatedAddress> sourceHeader = (SourceHeader<NatedAddress>) header;
                if (sourceHeader.getActualDestination().getParents().contains(selfAddress)) {
                    log.info("{} relaying message for:{}", new Object[]{selfAddress.getId(), sourceHeader.getSource()});
                    RelayHeader<NatedAddress> relayHeader = sourceHeader.getRelayHeader();
                    trigger(msg.copyMessage(relayHeader), network);
                    return;
                } else {
                    log.warn("{} received weird relay message:{} - dropping it", new Object[]{selfAddress.getId(), msg});
                    return;
                }
            } else if (header instanceof RelayHeader) {
                if (selfAddress.isOpen()) {
                    throw new RuntimeException("relay header msg received on open node - nat traversal logic error");
                }
                RelayHeader<NatedAddress> relayHeader = (RelayHeader<NatedAddress>) header;
                log.info("{} delivering relayed message:{} from:{}", new Object[]{selfAddress.getId(), msg, relayHeader.getActualSource()});
                Header<NatedAddress> originalHeader = relayHeader.getActualHeader();
                trigger(msg.copyMessage(originalHeader), local);
                return;
            } else {
                log.info("{} delivering direct message:{} from:{}", new Object[]{selfAddress.getId(), msg, header.getSource()});
                trigger(msg, local);
                return;
            }
        }

    };

    private Handler<NetMsg<Object>> handleOutgoingMsg = new Handler<NetMsg<Object>>() {

        @Override
        public void handle(NetMsg<Object> msg) {
            log.trace("{} sending msg:{}", new Object[]{selfAddress.getId(), msg});
            Header<NatedAddress> header = msg.getHeader();
            if (header.getDestination().isOpen() || header.getDestination().getParents().contains(selfAddress)) {
                log.info("{} sending direct message:{} to:{}", new Object[]{selfAddress.getId(), msg, header.getDestination()});
                trigger(msg, network);
                return;
            } else {
                if (header.getDestination().getParents().isEmpty()) {
                    // we removed the exception since a node with no parents will eventually get new parents
                    log.warn("{} nated node with no parents {}", selfAddress.getId(), header.getDestination().getId());
                    return;
                }
                NatedAddress parent = randomNode(header.getDestination().getParents());
                SourceHeader<NatedAddress> sourceHeader = new SourceHeader(header, parent);
                log.info("{} sending message:{} to relay:{}", new Object[]{selfAddress.getId(), msg, parent});
                trigger(msg.copyMessage(sourceHeader), network);
                return;
            }
        }

    };

    private Handler<CroupierSample> handleCroupierSample = new Handler<CroupierSample>() {
        @Override
        public void handle(CroupierSample event) {
            log.trace("{} croupier public nodes:{}", selfAddress.getBaseAdr(), event.publicSample);
            //If a parent hasn't replied to the heartbeats
            // we assign the node a different set of open parents
            // and inform Swim to spread the information to the other nodes
            if (!event.publicSample.isEmpty()) {
                publicSample.addAll(event.publicSample);
            }
            if (foundDeadParent && !changingParentsNow) {
                updateParents();
            }
        }
    };

    private void updateParents() {
        cancelPeriodicHeartbeat();
        changingParentsNow = true;
        Set<NatedAddress> parentPool = new HashSet<NatedAddress>();
        Iterator<Container<NatedAddress, Object>> it = publicSample.iterator();
        while (it.hasNext()) {
            // take the possible parents from croupier
            parentPool.add(it.next().getSource());
        }
        // add k=maximumParents parents from the parentpool 
        newParents.clear();
        for (int i = 0; i < maximumParents; i++) {
            NatedAddress newPar = randomNode(parentPool);
            if (newPar != null) {
                Set<NatedAddress> temp = new HashSet<NatedAddress>();
                temp.add(newPar);
                log.trace("{} sending parent request to {}", new Object[]{myid, newPar});
                NatedAddress sending = new BasicNatedAddress(new BasicAddress(localHost, 12345, myid), NatType.NAT, temp);
                trigger(new NetParentRequest(sending, newPar), network);
            }
        }
        scheduleParentChangeTimeout();
    }
    private final Handler<HeartbeatTimeout> handleHeartbeatTimeout = new Handler<HeartbeatTimeout>() {

        @Override
        public void handle(HeartbeatTimeout event) {
            // send heartbeats to all parents 
            // start a timeout for the reply
            for (NatedAddress open : selfAddress.getParents()) {
                trigger(new NetHeartbeat(selfAddress, open), network);
                scheduleHeartbeatReplyTimeout(open);
                log.trace("{} sending heartbeat to {} my parents: {}", new Object[]{selfAddress.getId(), open.getId(), selfAddress.getParents()});
            }
        }
    };

    private final Handler<HeartbeatReplyTimeout> handleHeartbeatReplyTimeout = new Handler<HeartbeatReplyTimeout>() {

        @Override
        public void handle(HeartbeatReplyTimeout event) {
            // if the timeout is triggered
            // declare that parent dead
            // pause the periodicHeartbeat 
            heartbeatTimeoutMap.remove(event.getParent());
            stopReplyTimeout();
            log.info("{} i found a Dead Parent: {}", selfAddress.getId(), event.getParent().getId());
            foundDeadParent = true;
            updateParents();

        }
    };

    private final Handler<NetHeartbeat> handleHeartbeat = new Handler<NetHeartbeat>() {

        @Override
        public void handle(NetHeartbeat event) {
            // A parent received the heartbeat and replies
            log.trace("{} sending heartbeatReply to {}", selfAddress.getId(), event.getSource());
            trigger(new NetHeartbeatReply(selfAddress, event.getSource()), network);
        }
    };

    private final Handler<NetHeartbeatReply> handleHeartbeatReply = new Handler<NetHeartbeatReply>() {

        @Override
        public void handle(NetHeartbeatReply event) {
            // reply for the heartbeat arrived, cancel its timeout
            log.trace("{} got reply from {}, canceling replytimeout", selfAddress.getId(), event.getSource().getId());
            cancelHeartbeatReplyTimeout(event.getSource());
        }
    };

    private final Handler<NetParentRequest> handleParentRequest = new Handler<NetParentRequest>() {

        @Override
        public void handle(NetParentRequest event) {
            // we could have a policy to not accept new children
            log.trace("{} received parent request", myid);
            trigger(new NetParentRequestAck(selfAddress, event.getSource()), network);
        }
    };

    private final Handler<NetParentRequestAck> handleParentRequestAck = new Handler<NetParentRequestAck>() {

        @Override
        public void handle(NetParentRequestAck event) {
            newParents.add(event.getSource());
        }
    };

    private final Handler<ParentChangeTimeout> handleParentChangeTimeout = new Handler<ParentChangeTimeout>() {

        @Override
        public void handle(ParentChangeTimeout event) {
            if (newParents.isEmpty()) {
                log.warn("{} Didn't find any new parents, i will try again later", selfAddress.getId());
                changingParentsNow = false;
            } else {
                // create a new object for the selfAddress
                // inform swim and schedule the heartbeats again
                Set<NatedAddress> temp = new HashSet<NatedAddress>();
                temp.addAll(newParents);
                selfAddress = new BasicNatedAddress(new BasicAddress(localHost, 12345, myid), NatType.NAT, temp);
                foundDeadParent = false;
                trigger(new NetParentChange(selfAddress, selfAddress), local);
                log.trace("{} Updated Parent Set {}", selfAddress.getId(), selfAddress.getParents().toString());
                schedulePeriodicHeartbeat();
            }
            newParents.clear();
        }
    };

    // gets a set of NatedAddress and returns a random NatedAddress
    private NatedAddress randomNode(Set<NatedAddress> nodes) {
        if (nodes.size() > 0) {
            int index = rand.nextInt(nodes.size());
            Iterator<NatedAddress> it = nodes.iterator();
            while (index > 0) {
                it.next();
                index--;
            }
            return it.next();
        }
        return null;
    }

    public static class NatTraversalInit extends Init<NatTraversalComp> {

        public final NatedAddress selfAddress;
        public final long seed;

        public NatTraversalInit(NatedAddress selfAddress, long seed) {
            this.selfAddress = selfAddress;
            this.seed = seed;
        }
    }

    // start periodic Heartbeat
    private void schedulePeriodicHeartbeat() {
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(5000, 5000);
        HeartbeatTimeout sc = new HeartbeatTimeout(spt);
        spt.setTimeoutEvent(sc);
        heartbeatTimeoutId = sc.getTimeoutId();
        trigger(spt, timer);
    }

    // stop periodic Heartbeat
    private void cancelPeriodicHeartbeat() {
        if (heartbeatTimeoutId != null) {
            CancelPeriodicTimeout cpt = new CancelPeriodicTimeout(heartbeatTimeoutId);
            trigger(cpt, timer);
            heartbeatTimeoutId = null;
        }
    }

    // HeartbeatTimeout is a simple periodic timeout
    private static class HeartbeatTimeout extends Timeout {

        public HeartbeatTimeout(SchedulePeriodicTimeout request) {
            super(request);
        }
    }

    // start heartbeat reply timeout
    private void scheduleHeartbeatReplyTimeout(NatedAddress peer) {
        ScheduleTimeout spt = new ScheduleTimeout(2000);
        HeartbeatReplyTimeout sc = new HeartbeatReplyTimeout(spt, peer);
        spt.setTimeoutEvent(sc);
        heartbeatTimeoutMap.put(peer, sc.getTimeoutId());
        trigger(spt, timer);
    }

    // stop heartbeat reply timeout
    private void cancelHeartbeatReplyTimeout(NatedAddress peer) {
        CancelTimeout cpt = new CancelTimeout(heartbeatTimeoutMap.get(peer));
        heartbeatTimeoutMap.remove(peer);
        trigger(cpt, timer);
    }

    private void stopReplyTimeout() {
        Set<NatedAddress> toStop = new HashSet<NatedAddress>();
        for (Map.Entry<NatedAddress, UUID> entry : heartbeatTimeoutMap.entrySet()) {
            toStop.add(entry.getKey());
        }
        for (NatedAddress peer : toStop) {
            cancelHeartbeatReplyTimeout(peer);
        }

    }

    // HeartbeatReplyTimeout gets the address so that we can stop it later 
    // if that parent replied
    private static class HeartbeatReplyTimeout extends Timeout {

        private final NatedAddress parent;

        public HeartbeatReplyTimeout(ScheduleTimeout request, NatedAddress parent) {
            super(request);
            this.parent = parent;
        }

        public NatedAddress getParent() {
            return parent;
        }
    }

    // start parnentChange reply timeout
    private void scheduleParentChangeTimeout() {
        ScheduleTimeout spt = new ScheduleTimeout(2000);
        ParentChangeTimeout sc = new ParentChangeTimeout(spt);
        spt.setTimeoutEvent(sc);
        trigger(spt, timer);
    }

    // ParentChangeTimeout is a simple timeout
    private static class ParentChangeTimeout extends Timeout {

        public ParentChangeTimeout(ScheduleTimeout request) {
            super(request);
        }
    }
}
