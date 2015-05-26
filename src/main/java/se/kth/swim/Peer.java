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

import se.sics.p2ptoolbox.util.network.NatedAddress;

/**
 *
 * @author Nick
 */
public class Peer implements Comparable<Peer>{
    private NatedAddress peer;

	
	public Peer(NatedAddress peer)  {
		this.peer = peer;
	}
	
	public NatedAddress getPeer() {
		return peer;
	}
        
        @Override
	public int hashCode() {
		return peer.getId().hashCode();
	}
        @Override
	public boolean equals(Object obj) {
		if (obj instanceof Peer) {
			Peer m = (Peer) obj;
			return m.peer.getId().equals(peer.getId());
		}
		return false;
	}

    public int compareTo(Peer o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    @Override
	public String toString() {
		return String.format(peer.toString());
	}
}
