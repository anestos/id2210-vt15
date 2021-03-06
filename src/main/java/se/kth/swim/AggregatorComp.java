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

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.swim.msg.net.NetStatus;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.p2ptoolbox.util.network.NatedAddress;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class AggregatorComp extends ComponentDefinition {

    private static final Logger log = LoggerFactory.getLogger(AggregatorComp.class);
    private Positive<Network> network = requires(Network.class);
    private Positive<Timer> timer = requires(Timer.class);
    private Set<Peer> peersWithAllTheInfo = new HashSet<Peer>();

    private final NatedAddress selfAddress;
    public final List<Integer> nodesToStartList;
    public final List<Integer> nodesToKillList;

    private int startTime;
    private int endTime;
    private boolean converged = false;
    private boolean timerStarted = false;
    private int customTimer = 0;

    public AggregatorComp(AggregatorInit init) {
        this.selfAddress = init.selfAddress;
        this.nodesToKillList = init.nodesToKillList;
        this.nodesToStartList = init.nodesToStartList;
        log.info("{} initiating...", new Object[]{selfAddress.getId()});

        subscribe(handleStart, control);
        subscribe(handleStop, control);
        subscribe(handleStatus, network);
        subscribe(handleTimerTimeout, timer);

    }

    private Handler<Start> handleStart = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            log.info("{} starting...", new Object[]{selfAddress});
            // start a timer to count simulation time
            schedulePeriodicTimer();
        }

    };
    private Handler<Stop> handleStop = new Handler<Stop>() {

        @Override
        public void handle(Stop event) {
            log.info("{} stopping...", new Object[]{selfAddress});
        }

    };

    private Handler<NetStatus> handleStatus = new Handler<NetStatus>() {

        @Override
        public void handle(NetStatus status) {
//            log.info("{} status from:{} pings:{}, peers d:{} a:{} s:{}", new Object[]{selfAddress.getId(), status.getHeader().getSource(), status.getContent().receivedPings, status.getContent().deadPeers, status.getContent().alivePeers, status.getContent().suspectedPeers});
            if (!timerStarted && status.getContent().deadPeers.size() > 0) {
                // Save the time when one node sends the aggregator that it detected a dead node in the system
                startTime = customTimer;
                timerStarted = true;
            }
            if (!nodesToKillList.contains(status.getSource().getId()) && !peersWithAllTheInfo.contains(new Peer(status.getSource())) && !converged) {
                // When a node who is expected to be alive at the end has the correct information
                // we put it in the peersWithAllTheInfo set
                if (status.getContent().deadPeers.size() == nodesToKillList.size()) {
                    boolean check = true;
                    for (Peer peerToCheck : status.getContent().deadPeers) {
                        if (!nodesToKillList.contains(peerToCheck.getPeer().getId())) {
                            check = false;
                        }
                    }
                    if (check) {
                        peersWithAllTheInfo.add(new Peer(status.getSource()));
                    }
                    if (peersWithAllTheInfo.size() == nodesToStartList.size() - nodesToKillList.size()) {
                        // when the size of peersWithAllTheInfo is the expected 
                        // the system converges so we print out the time it took
                        endTime = customTimer;
                        int diff = endTime - startTime;
                        converged = true;
                        log.info("System converged in: {} ms", new Object[]{diff*100});
                    }
                }
            } else if (!nodesToKillList.contains(status.getSource().getId()) && peersWithAllTheInfo.contains(new Peer(status.getSource())) && !converged) {
                // A node might have false information after it has the correct 
                // so we remove it from the peersWithAllTheInfo set
                if (status.getContent().deadPeers.size() == nodesToKillList.size()) {
                    boolean check = true;
                    for (Peer peerToCheck : status.getContent().deadPeers) {
                        if (!nodesToKillList.contains(peerToCheck.getPeer().getId())) {
                            check = false;
                        }
                    }
                    if (!check) {
                        peersWithAllTheInfo.remove(new Peer(status.getSource()));
                    }
                } else {
                    peersWithAllTheInfo.remove(new Peer(status.getSource()));
                }
            }
        }
    };

    public static class AggregatorInit extends Init<AggregatorComp> {

        public final NatedAddress selfAddress;
        public final List<Integer> nodesToStartList;
        public final List<Integer> nodesToKillList;

        public AggregatorInit(NatedAddress selfAddress, List<Integer> nodesToStartList, List<Integer> nodesToKillList) {
            this.selfAddress = selfAddress;
            this.nodesToStartList = nodesToStartList;
            this.nodesToKillList = nodesToKillList;
        }
    }
        private Handler<TimerTimeout> handleTimerTimeout = new Handler<TimerTimeout>() {

        @Override
        public void handle(TimerTimeout event) {
            // this counter counts time each 100ms in simulation time
            customTimer++;
        }
    };
       private void schedulePeriodicTimer() {
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(0, 100);
        TimerTimeout sc = new TimerTimeout(spt);
        spt.setTimeoutEvent(sc);
        trigger(spt, timer);
    }
       
           private static class TimerTimeout extends Timeout {

        public TimerTimeout(SchedulePeriodicTimeout request) {
            super(request);
        }
    }
}
