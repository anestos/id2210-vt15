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

package se.kth.swim.msg;

import java.util.Set;
import se.kth.swim.Peer;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class Status {
    public int receivedPings;
    public Set<Peer> deadPeers;
    public Set<Peer> alivePeers;
    public Set<Peer> suspectedPeers;
    
    public Status(int receivedPings, Set<Peer> deadPeers, Set<Peer> alivePeers, Set<Peer> suspectedPeers) {
        this.receivedPings = receivedPings;
        this.deadPeers = deadPeers;
        this.alivePeers = alivePeers;
        this.suspectedPeers = suspectedPeers;
    }
}
