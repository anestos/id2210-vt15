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
package se.kth.swim.simulation;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import se.kth.swim.AggregatorComp;
import se.kth.swim.HostComp;
import se.kth.swim.croupier.CroupierConfig;
import se.sics.p2ptoolbox.simulator.cmd.OperationCmd;
import se.sics.p2ptoolbox.simulator.cmd.impl.SimulationResult;
import se.sics.p2ptoolbox.simulator.cmd.impl.StartAggregatorCmd;
import se.sics.p2ptoolbox.simulator.cmd.impl.StartNodeCmd;
import se.sics.p2ptoolbox.simulator.cmd.impl.KillNodeCmd;
import se.sics.p2ptoolbox.simulator.dsl.SimulationScenario;
import se.sics.p2ptoolbox.simulator.dsl.adaptor.Operation;
import se.sics.p2ptoolbox.simulator.dsl.adaptor.Operation1;
import se.sics.p2ptoolbox.simulator.dsl.distribution.ConstantDistribution;
import se.sics.p2ptoolbox.simulator.dsl.distribution.extra.GenIntSequentialDistribution;
import se.sics.p2ptoolbox.util.network.NatType;
import se.sics.p2ptoolbox.util.network.NatedAddress;
import se.sics.p2ptoolbox.util.network.impl.BasicAddress;
import se.sics.p2ptoolbox.util.network.impl.BasicNatedAddress;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class SwimScenario1 {

    private static long seed;
    private static InetAddress localHost;

    private static CroupierConfig croupierConfig = new CroupierConfig(10, 5, 2000, 1000);

    static {
        try {
            localHost = InetAddress.getByName("127.0.0.1");
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        }
    }
    private static Integer[] nodesToStart;
    private static List<Integer> nodesToStartList;

    static {
        nodesToStart = new Integer[100];
        for (int i = 0; i < 100; i++) {
            nodesToStart[i] = i * 2;
        }
        nodesToStartList = Arrays.asList(nodesToStart);
    }
    private static Integer[] nodesToKill;
    private static List<Integer> nodesToKillList;

    static {
        nodesToKill = new Integer[10];
        for (int i = 0; i < 10; i++) {
            nodesToKill[i] = i * 2;
        }
        nodesToKillList = Arrays.asList(nodesToKill);

    }

    static Operation1<StartAggregatorCmd, Integer> startAggregatorOp = new Operation1<StartAggregatorCmd, Integer>() {

        //@Override
        public StartAggregatorCmd generate(final Integer nodeId) {
            return new StartAggregatorCmd<AggregatorComp, NatedAddress>() {
                private NatedAddress aggregatorAddress;

                // @Override
                public Class getNodeComponentDefinition() {
                    return AggregatorComp.class;
                }

                //@Override
                public AggregatorComp.AggregatorInit getNodeComponentInit() {
                    aggregatorAddress = new BasicNatedAddress(new BasicAddress(localHost, 23456, nodeId));
                    return new AggregatorComp.AggregatorInit(aggregatorAddress, nodesToStartList, nodesToKillList);
                }

                //  @Override
                public NatedAddress getAddress() {
                    return aggregatorAddress;
                }

            };
        }
    };

    static Operation1<StartNodeCmd, Integer> startNodeOp = new Operation1<StartNodeCmd, Integer>() {

        // @Override
        public StartNodeCmd generate(final Integer nodeId) {
            return new StartNodeCmd<HostComp, NatedAddress>() {
                private NatedAddress nodeAddress;

                // @Override
                public Class getNodeComponentDefinition() {
                    return HostComp.class;
                }

                //  @Override
                public HostComp.HostInit getNodeComponentInit(NatedAddress aggregatorServer, Set<NatedAddress> bootstrapNodes) {
                    if (nodeId % 2 == 0) {
                        //open address
                        nodeAddress = new BasicNatedAddress(new BasicAddress(localHost, 12345, nodeId));
                    } else {
                        //nated address
                        nodeAddress = new BasicNatedAddress(new BasicAddress(localHost, 12345, nodeId), NatType.NAT, bootstrapNodes);
                    }
                    /**
                     * we don't want all nodes to start their pseudo random
                     * generators with same seed else they might behave the same
                     */
                    long nodeSeed = seed + nodeId;
                    return new HostComp.HostInit(nodeAddress, bootstrapNodes, aggregatorServer, nodeSeed, croupierConfig);
                }

                // @Override
                public Integer getNodeId() {
                    return nodeId;
                }

                // @Override
                public NatedAddress getAddress() {
                    return nodeAddress;
                }

                //  @Override
                public int bootstrapSize() {
                    return 5;
                }

            };
        }
    };

    static Operation1<KillNodeCmd, Integer> killNodeOp = new Operation1<KillNodeCmd, Integer>() {

        public KillNodeCmd generate(final Integer nodeId) {
            return new KillNodeCmd() {
                public Integer getNodeId() {
                    return nodeId;
                }
            };
        }

    };

    static Operation<SimulationResult> simulationResult = new Operation<SimulationResult>() {

        public SimulationResult generate() {
            return new SimulationResult() {

                //  @Override
                public void setSimulationResult(OperationCmd.ValidationException failureCause) {
                    SwimSimulationResult.failureCause = failureCause;
                }
            };
        }
    };

    //Operations require Distributions as parameters
    //1.ConstantDistribution - this will provide same parameter no matter how many times it is called
    //2.BasicIntSequentialDistribution - on each call it gives the next int. Works more or less like a counter
    //3.GenIntSequentialDistribution - give it a vector. It will draw elements from it on each call. 
    //Once out of elements it will give null. 
    //So be carefull for null pointer exception if you draw more times than elements
    //check se.sics.p2ptoolbox.simulator.dsl.distribution for more distributions
    //you can implement your own - by extending Distribution
    public static SimulationScenario simpleBoot(final long seed) {
        SwimScenario1.seed = seed;
        SimulationScenario scen = new SimulationScenario() {
            {
                StochasticProcess startAggregator = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(1, startAggregatorOp, new ConstantDistribution(Integer.class, 0));
                    }
                };

                StochasticProcess startPeers = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(100, startNodeOp, new GenIntSequentialDistribution(nodesToStart));
                    }
                };

                StochasticProcess killPeers = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(10, killNodeOp, new GenIntSequentialDistribution(nodesToKill));
                    }
                };

                StochasticProcess fetchSimulationResult = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(1000));
                        raise(1, simulationResult);
                    }
                };

                startAggregator.start();
                startPeers.startAfterTerminationOf(1000, startAggregator);
                killPeers.startAfterTerminationOf(19000, startPeers);
                fetchSimulationResult.startAfterTerminationOf(1000000, startPeers);
                terminateAfterTerminationOf(1000, fetchSimulationResult);

            }
        };

        scen.setSeed(seed);

        return scen;
    }
}
