package {
	import flash.display.Sprite;
	import flash.events.Event;
	import flash.events.IOErrorEvent;
	import flash.events.ProgressEvent;
	import flash.events.SecurityErrorEvent;
	import flash.external.ExternalInterface;
	import flash.net.Socket;
	import flash.system.Security;
	import flash.utils.ByteArray;

	public class ZLibSocket extends Sprite {
		public function ZLibSocket() {
			if (ExternalInterface.available) {
				ExternalInterface.addCallback("send", send);
				ExternalInterface.addCallback("connect", connect);
				ExternalInterface.addCallback("disconnect", disconnect);
			}
		}

		private function call(name:String, param:Object = null):void {
			if (ExternalInterface.available) {
				ExternalInterface.call("ZLibSocket_" + name, param);
			}
		}

		private function onConnect(e:Event):void {
			call("Connect");
		}

		private function onClose(event:Event):void {
			disconnect();
			call("Disconnect");
		}

		private function onIoError(e:IOErrorEvent):void {
			disconnect();
			call("Error", "IO");
		}

		private function onSecurityError(e:SecurityErrorEvent):void {
			disconnect();
			call("Error", "SECURITY");
		}

		private var socket:Socket = null;
		private var buffer:ByteArray = new ByteArray();
		private var packetSize:int = 0;
		private var packets:Array = new Array();

		private function getPacketSize(ba:ByteArray):int {
			if (ba.length < 4) {
				return 0;
			}
			if ((ba[2] * 256 + ba[3]) % 31 != 0) {
				disconnect();
				call("Error", "PACKET");
				return 0;
			}
			return ba[0] * 256 + ba[1];
		}

		private function onRecvPacket(packet:ByteArray):void {
			var data:ByteArray = new ByteArray();
			packet.readShort();
			packet.readBytes(data);
			try {
				data.uncompress();
			} catch (e:Error) {
				disconnect();
				call("Error", "UNCOMPRESS");
				return;
			}
			call("Recv", data.readUTFBytes(data.length).split("\\").join("\\\\"));
		}

		private function onSocketData(e:ProgressEvent):void {
			var packet:ByteArray;
			socket.readBytes(buffer, buffer.length);
			while (packetSize > 0 || (packetSize = getPacketSize(buffer)) > 0) {
				if (buffer.length < packetSize) {
					return;
				}
				packet = new ByteArray();
				buffer.readBytes(packet, 0, packetSize);
				packets.push(packet);
				var temp:ByteArray = new ByteArray();
				buffer.readBytes(temp);
				buffer = temp;
				packetSize = 0;
			}
			while ((packet = packets.shift()) != null) {
				onRecvPacket(packet);
			}
		}

		private function send(s:String):void {
			if (socket == null) {
				call("Error", "CLOSE");
				return;
			}
			var ba:ByteArray = new ByteArray();
			ba.writeUTFBytes(s);
			ba.compress();
			socket.writeShort(ba.length + 2);
			socket.writeBytes(ba);
			socket.flush();
		}

		private function connect(host:String, port:int, policyPort:int = 0):void {
			if (socket != null) {
				call("Error", "CONNECT");
				return;
			}
			if (policyPort > 0) {
				Security.loadPolicyFile("xmlsocket://" + host + ":" + policyPort);
			}
			socket = new Socket();
			socket.addEventListener(Event.CONNECT, onConnect);
			socket.addEventListener(Event.CLOSE, onClose);
			socket.addEventListener(ProgressEvent.SOCKET_DATA, onSocketData);
			socket.addEventListener(IOErrorEvent.IO_ERROR, onIoError);
			socket.addEventListener(SecurityErrorEvent.SECURITY_ERROR, onSecurityError);
			socket.connect(host, port);
		}

		private function disconnect():void {
			if (socket == null) {
				call("Error", "CLOSE");
				return;
			}
			socket.removeEventListener(Event.CONNECT, onConnect);
			socket.removeEventListener(Event.CLOSE, onClose);
			socket.removeEventListener(ProgressEvent.SOCKET_DATA, onSocketData);
			socket.removeEventListener(IOErrorEvent.IO_ERROR, onIoError);
			socket.removeEventListener(SecurityErrorEvent.SECURITY_ERROR, onSecurityError);
			socket.close();
			socket = null;
		}
	}
}