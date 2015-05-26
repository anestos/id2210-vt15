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
package se.kth.swim.msg.net;

import se.kth.swim.PeerStatus;
import se.kth.swim.StateChanges;
import se.kth.swim.msg.IndirectPing;
import se.sics.kompics.network.Header;
import se.sics.p2ptoolbox.util.network.NatedAddress;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class NetIndirectPing extends NetMsg<IndirectPing> {

    public NetIndirectPing(NatedAddress src, NatedAddress dst, NatedAddress original, StateChanges<PeerStatus> queue) {
        super(src, dst, new IndirectPing(original, queue));
    }

    private NetIndirectPing(Header<NatedAddress> header, IndirectPing content) {
        super(header, content);
    }

    @Override
    public NetMsg copyMessage(Header<NatedAddress> newHeader) {
        return new NetIndirectPing(newHeader, getContent());
    }

}
