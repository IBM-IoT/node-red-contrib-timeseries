node-red-contrib-timeseries
========================
A suite of [Node-RED](http://nodered.org) nodes providing an easy-to-use interface for working with a TimeSeries database.

Install
-------
Run the following command in the root directory of your Node-RED install:

```npm install node-red-contrib-timeseries```

Once installed, the TimeSeries input and output nodes will be available in the
node pallet.


TimeSeries Database Requirements
------
You must have a TimeSeries database running with the wire listener installed
and running in REST mode with SQL passthrough turned on.

See the following documentation for information regarding the setup and configuration of the wire listener:

http://www-01.ibm.com/support/knowledgecenter/SSGU8G_12.1.0/com.ibm.json.doc/ids_json_004.htm


Overview
-----
Five nodes are included in this install. The TimeSeries input and output nodes.  Timeseries simple input and output nodes, and an IoTF Transformation node.

For more information on the input and output nodes see the README.md file in the timeseries folder.  For more information on the IoTF Transformation node see the iotf-transform folder.
