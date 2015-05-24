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

/**
 *
 * @author Nikos
 */
public class PeerStatus implements Comparable<PeerStatus> {

    private Peer peer;
    private String status;
    private int incarnationNumber;
    private int lamdaCounter;

    public PeerStatus(Peer peer, String status, int incarnationNumber, int lamdaCounter) {
        this.peer = peer;
        this.status = status;
        this.incarnationNumber = incarnationNumber;
        this.lamdaCounter = lamdaCounter;
    }

    public String getStatus() {
        return status;
    }

    public Peer getPeer() {
        return peer;
    }

    public int getLamdaCounter() {
        return lamdaCounter;
    }
    
    public int increaseLamdaCounter(){
        lamdaCounter++;
        return lamdaCounter;
    }

    public int getIncarnationNumber() {
        return incarnationNumber;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PeerStatus) {
            PeerStatus m = (PeerStatus) obj;
            return m.peer.getPeer().equals(peer.getPeer()) && m.status.equals(status);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return peer.hashCode();
    }

    @Override
    public String toString() {
        return String.format(peer.toString() + "[" + status.toString() + " | " + incarnationNumber + "]");
    }

    public int compareTo(PeerStatus o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
