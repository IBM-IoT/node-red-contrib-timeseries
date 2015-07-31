node-red-contrib-timeseries
========================
A suite of [Node-RED](http://nodered.org) nodes providing an easy-to-use interface for working with a TimeSeries database.

Install
-------
Run the following command in the root directory of your Node-RED install:

```npm install node-red-contrib-timeseries```

Once installed, the nodes will be available in the node pallet.


Overview
-----
Five nodes are included in this install:
 
- TimeSeries Input Node
- TimeSeries Output Node
- Simplified TimeSeries Input Node
- Simplified TimeSeries Output Node
- Internet of Things Foundation (IoTF) Transformation node

For more information on the TimeSeries input and output nodes see the README.md file in the ```timeseries``` folder.

For more information on the IoTF Transformation node see the ```iotf-transform``` folder.


TimeSeries Database Requirements
------
You must have a TimeSeries database running with the wire listener installed
and running in REST mode with SQL passthrough turned on.

See the following documentation for information regarding the setup and configuration of the wire listener:

http://www-01.ibm.com/support/knowledgecenter/SSGU8G_12.1.0/com.ibm.json.doc/ids_json_004.htm
