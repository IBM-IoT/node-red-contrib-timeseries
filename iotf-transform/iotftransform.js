module.exports = function(RED) {
	RED.nodes.registerType("iotf-transform", transform);
	
	function transform(n) {
		RED.nodes.createNode(this,n);
		var node = this;

		node.on('input', function(msg) {
			msg.payload.d = msg.payload.d[0];
			msg.payload.d.id = msg.payload.id;
			msg.payload.ts = msg.payload.d.timestamp;
			delete msg.payload.d.timestamp;
			node.send(msg);
		});
	}
}
