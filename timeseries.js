/**
 * Copyright 2015 IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/


module.exports = function(RED) {
    "use strict";
    var request = require('request');
    var http = require('http');
    var urllib = require("url");

    function TimeSeriesNode(n) {
        RED.nodes.createNode(this,n);
        this.hostname = n.hostname;
        this.port = n.port;
        this.db = n.db;
        this.name = n.name;
    }

    RED.nodes.registerType("timeseries",TimeSeriesNode,{
        credentials: {
            user: {type:"text"},
            password: {type: "password"}
        }
    });

    function TimeSeriesOutNode(n) {
        RED.nodes.createNode(this,n);
        this.baseTimeSeriesTable = n.baseTimeSeriesTable;
        this.timeseries = n.timeseries;
        this.timeseriesConfig = RED.nodes.getNode(this.timeseries);
        this.host = this.timeseriesConfig.hostname;
        this.port = this.timeseriesConfig.port;
        this.db = this.timeseriesConfig.db;
        var node = this;

        var baseUrl =  "".concat("http://", this.host, ":", this.port, "/", this.db, "/");
        var sqlPassThroughBaseUrl = baseUrl.concat('system.sql?query=');

        // Create a virtual table (VTI) off of the base TimeSeries table.
        // We will use this virtual table for the easy insertion of records through the REST API.
        var virtualTableName = node.baseTimeSeriesTable.concat("_v");
        createVirtualTable(virtualTableName);

        node.on("input",function(msg) {
            // Make the incoming payload a JSON object if it is not already.
            if (typeof msg.payload === "string"){
                msg.payload = JSON.parse(msg.payload);
            }
            if (typeof msg.payload !== "object") {
                msg.payload = {"payload": msg.payload};
            }

            // Do the REST POST call that inserts the value into the Virtual table.
            var url = "".concat(baseUrl, virtualTableName);
            var opts = urllib.parse(url);
            var method = "POST";
            opts.method = "POST";
            opts.headers = {};

            if (this.credentials && this.credentials.user) {
                opts.auth = this.credentials.user+":"+(this.credentials.password||"");
            }
            var payload = null;

            if (msg.payload && (method == "POST") ) {
                if (typeof msg.payload === "string" || Buffer.isBuffer(msg.payload)) {
                    payload = msg.payload;
                } else if (typeof msg.payload == "number") {
                    payload = msg.payload+"";
                } else {
                    if (opts.headers['content-type'] == 'application/x-www-form-urlencoded') {
                        payload = querystring.stringify(msg.payload);
                    } else {
                        payload = JSON.stringify(msg.payload);
                        if (opts.headers['content-type'] == null) {
                            opts.headers['content-type'] = "application/json";
                        }
                    }
                }
                if (opts.headers['content-length'] == null) {
                    opts.headers['content-length'] = Buffer.byteLength(payload);
                }
            }

            var req = ((/^https/.test(url))?https:http).request(opts,function(res) {
                (node.ret === "bin") ? res.setEncoding('binary') : res.setEncoding('utf8');
                msg.statusCode = res.statusCode;
                msg.headers = res.headers;
                msg.payload = "";
                res.on('data',function(chunk) {
                    msg.payload += chunk;
                });
                res.on('end',function() {
                    if (node.ret === "bin") {
                        msg.payload = new Buffer(msg.payload,"binary");
                    }
                    else if (node.ret === "obj") {
                        try { msg.payload = JSON.parse(msg.payload); }
                        catch(e) { node.warn("JSON parse error"); }
                    }
                    node.send(msg);
                    node.status({});
                });
            });
            req.on('error',function(err) {
                msg.payload = err.toString() + " : " + url;
                msg.statusCode = err.code;
                node.warning(err.toString());
                node.send(msg);
                node.status({fill:"red",shape:"ring",text:err.code});
            });
            if (payload) {
                req.write(payload);
            }
            req.end();
        });

        function createVirtualTable(virtualTableName) {
            var virtualTableCreationJson = "".concat('{"$sql":"execute procedure tscreatevirtualtab(',
                "'", virtualTableName, "', '", node.baseTimeSeriesTable, "')", ';"}');
            var virtualTableCreationUrl = sqlPassThroughBaseUrl.concat(virtualTableCreationJson);
            node.log("If it does not already exist, creating the virtual table for the base table: " +
            node.baseTimeSeriesTable);
            node.log(virtualTableCreationUrl);
            // Execute the command via REST.
            request(virtualTableCreationUrl, function (error, response, body) {
                if (!error && response.statusCode == 200) {
                    if (body.indexOf("already exists in database") > -1) {
                        node.log("Virtual table " + virtualTableName + " already exists. Nothing to do.")
                    }
                    else {
                        node.log(body);
                    }
                }
                else {
                    node.error(error);
                }
            })
        }

    }
    RED.nodes.registerType("timeseries out", TimeSeriesOutNode);

    function TimeSeriesInNode(n) {
        RED.nodes.createNode(this,n);
        this.baseTimeSeriesTable = n.baseTimeSeriesTable;
        this.timeseries = n.timeseries;
        this.tscolumn = n.tscolumn;
        this.unit = n.unit;
        this.range = n.range;
        this.calendarRange = n.calendarRange;
        this.calendarUnit = n.calendarUnit;
        this.ids = n.ids;
        this.timeseriesConfig = RED.nodes.getNode(this.timeseries);
        this.host = this.timeseriesConfig.hostname;
        this.port = this.timeseriesConfig.port;
        this.db = this.timeseriesConfig.db;
        this.timeType = n.timeType;
        this.mode = n.mode;
        this.rules = n.rules;
        this.timeseriesConfig = RED.nodes.getNode(this.timeseries);

        if (this.timeseriesConfig) {

            var node = this;

            var baseUrl =  "".concat("http://", this.host, ":", this.port, "/", this.db, "/");
            var sqlPassThroughBaseUrl = baseUrl.concat('system.sql?query=');

            var original_unit = this.calendarUnit;
            var moment = require('moment');

            if (this.calendarUnit === "minute"){
                this.calendarUnit = "min";
            }
            else if (this.calendarUnit === "second") {
                this.calendarUnit = "sec";
            }


            var abbreviatedUnit = "";
            if (this.calendarUnit === "minute") {
                abbreviatedUnit = "min";
            }
            else if (this.calendarUnit === "second") {
                abbreviatedUnit = "sec";
            }
            else {
                abbreviatedUnit = this.calendarUnit;
            }

            var newCalendarName = "".concat("ts_", this.calendarRange, abbreviatedUnit);

            // Create any calendars we need.
            // First, create the calendar pattern.
            var calendarRangeMinusOne = (parseInt(this.calendarRange) - 1).toString();
            var calendarPatternName = "".concat("ts_", this.calendarRange, this.calendarUnit);
            var createCalendarPatternUrl = sqlPassThroughBaseUrl.concat('{"$sql":' +
            '"INSERT INTO CalendarPatterns values (', "'", calendarPatternName,"'," +
            "'{1 on ,  ", calendarRangeMinusOne,  " off}, ", original_unit, "')", '"}');

            node.log("If it does not already exist, creating the calendar pattern: " + calendarPatternName);
            node.log(createCalendarPatternUrl);
            request(createCalendarPatternUrl, this, function (error, response, body) {
                if (!error && response.statusCode == 200) {
                    if (body.indexOf("duplicate value in a UNIQUE INDEX column") > -1) {
                        node.log("Calendar pattern " + calendarPatternName + " already exists. Nothing to do.")
                    }
                    else {
                        node.log(body);
                    }
                }
                else {
                    node.log(body);
                    node.log(error);
                }

                var newCalendarName = "".concat("ts_", this.calendarRange, abbreviatedUnit);
                var createCalenderUrl = sqlPassThroughBaseUrl.concat('{"$sql":' +
                '"INSERT INTO CalendarTable(c_name, c_calendar)' +
                "values ('", newCalendarName , "', ", "'startdate(2011-01-01 00:00:00)," +
                "pattstart(2011-01-01 00:00:00), pattname(", calendarPatternName, ")')", '"}');
                node.log("If it does not already exist, creating the calendar pattern: " + newCalendarName);
                node.log(createCalenderUrl);
                request(createCalenderUrl, function (error, response, body) {
                    if (!error && response.statusCode == 200) {

                        if (body.indexOf("duplicate value in a UNIQUE INDEX column") > -1){
                            node.log("Calendar " + newCalendarName + " already exists. Nothing to do.")
                        }
                        else{
                            node.log(body);
                        }
                    }
                    else {
                        node.log(body);
                        node.log(error);
                    }
                })
            })

            node.on("input", function(msg) {

                var id_field_name = this.ids;


                var id_field_value = msg.payload[this.ids];

                if (Array.isArray(id_field_value)){
                    id_field_value = "'" + id_field_value.join("','") + "'";
                }
                else{
                    id_field_value = "'" + id_field_value + "'";
                }
                id_field_value = id_field_value.toString();

                var current_date;
                var past_date;
                var date_time_format = "YYYY-MM-DD HH:mm:ss";
                if (node.timeType === 'current_time'){
                    current_date = moment().format(date_time_format);
                    past_date = moment(current_date).subtract(parseInt(node.range), node.unit).format(date_time_format);
                }
                else {
                    current_date = moment(msg.payload['timestamp']).format(date_time_format);
                    past_date = moment(msg.payload['timestamp']).subtract(parseInt(node.range), node.unit).format(date_time_format);
                }

                var outRowFieldString = "";
                for (var i = 0; i < node.rules.length; i++){
                    var field_name = node.rules[i].v;
                    if (field_name.indexOf('.') !== -1 ){
                        field_name = field_name.substring(field_name.indexOf(".") + 1);
                    }

                    outRowFieldString = outRowFieldString.concat(field_name, "_", node.rules[i].t,  " float,")
                }

                outRowFieldString = outRowFieldString.replace(/,+$/, "");
                outRowFieldString = outRowFieldString.replace(/\./g,'_');

                var outrow = "".concat('timestamp datetime year to fraction(5), ', outRowFieldString);

                var aggregation_string = "";
                for (var i = 0; i < node.rules.length; i++){
                    aggregation_string = aggregation_string.concat(node.rules[i].t, "($", node.rules[i].v, "),")
                }

                aggregation_string = aggregation_string.replace(/,+$/, "");

                var aggregation_string_for_outer_aggregateBy = "";
                for (var i = 0; i < node.rules.length; i++){
                    var field_name = node.rules[i].v;
                    if (field_name.indexOf('.') !== -1 ){
                        field_name = field_name.substring(field_name.indexOf(".") + 1);
                    }
                    aggregation_string_for_outer_aggregateBy = aggregation_string_for_outer_aggregateBy.concat(node.rules[i].t, "($", field_name.replace(/\./g,'_'), "_", node.rules[i].t , "),")
                }

                aggregation_string_for_outer_aggregateBy = aggregation_string_for_outer_aggregateBy.replace(/,+$/, "");

                if (node.mode === "discrete") {
                    var aggregateByFunctionUrl = "".concat(baseUrl,
                        'system.sql?query=' +
                        '{"$sql":"SELECT t.* from table (transpose ((SELECT AggregateBy(',
                        "'", aggregation_string, "','",
                        newCalendarName,
                        "',", node.tscolumn, ",",
                        "0",
                        ",'", past_date, ".00000'::datetime year to fraction(5)",
                        ",'", current_date, ".00000'::datetime year to fraction(5))::timeseries( row (", outrow, ")) from ",
                        node.baseTimeSeriesTable, ' where ', id_field_name, ' in (', id_field_value, ")", ' ))) as tab(t);"}');

                    node.log("Calling the aggregateBy() function");
                    node.log(aggregateByFunctionUrl);

                    request(aggregateByFunctionUrl, function (error, response, body) {
                        if (!error && response.statusCode == 200) {
                            node.log("The aggregateBy() function returned" + body);
                            msg.payload = formatPayloadToSend(body, id_field_name, id_field_value);

                            // Send out whatever response we get back.
                            node.send(msg);
                        }
                        else {
                            node.log(body);
                            node.log(error);
                        }
                    })
                }

                else if (node.mode === "continuous"){

                    var stat = "AVG".concat("($", node.tscolumnvalue, ")");

                    var aggregation_string_only_avg = aggregation_string.replace("SUM", "AVG").replace("MIN", "AVG").replace("MAX", "AVG");

                    var aggregateByFunctionUrl = "".concat(baseUrl,
                        'system.sql?query=' +
                        '{"$sql":"' +
                        'SELECT t.*' +
                        ' from table ' +
                        '(transpose ((' +
                        'SELECT AggregateBy(',
                        "'", aggregation_string_for_outer_aggregateBy, "', '",
                        newCalendarName,
                        "', AggregateBy(","'", aggregation_string_only_avg, "','",
                        getCalenderTypeForInnerAggregateBy(node.calendarRange, node.calendarUnit),
                        "',", node.tscolumn, ", 0 ",
                        ",'", past_date, ".00000'::datetime year to fraction(5)",
                        ",'", current_date, ".00000'::datetime year to fraction(5))::timeseries( row (", outrow, ")), 1) from ",
                        node.baseTimeSeriesTable, ' where ', id_field_name, ' in ', "(", id_field_value, ")", ' ))) as tab(t);"}');
                    //node.baseTimeSeriesTable, ' where ', id_field_name, " = 'eca86bf831dd.ZWnode3' or  ", id_field_name, " = 'eca86bf831dd.ZWnode2'" ,  ' ))) as tab(t);"}');

                    node.log("Calling the aggregateBy() function");
                    node.log(aggregateByFunctionUrl);

                    request(aggregateByFunctionUrl, function (error, response, body) {
                        if (!error && response.statusCode == 200) {
                            node.log("The aggregateBy() function returned" + body);

                            msg.payload = formatPayloadToSend(body, id_field_name, id_field_value);

                            // Send out whatever response we get back.
                            node.send(msg);
                        }
                        else {
                            node.log(body);
                            node.log(error);
                        }
                    })

                }
                else {
                    node.warn("Invalid mode specified. Invalid state reached.");
                }

            });
        }
        this.on("close", function() {
            if (this.clientDb) {
                this.clientDb.close();
            }
        });
    }
    RED.nodes.registerType("timeseries in",TimeSeriesInNode);
}

function getCalenderTypeForInnerAggregateBy(calendarValue, calendarUnit) {

    if (calendarValue === 0){
        console.error("0 is not a valid calendar value.");
    }

    var calender_in_seconds;
    if (calendarUnit === 'sec'){
        calender_in_seconds = calendarValue;
    }
    else if (calendarUnit === 'min') {
        calender_in_seconds = calendarValue * 60;
    }
    else if (calendarUnit === 'hour') {
        calender_in_seconds = calendarValue * 3600;
    }
    else if (calendarUnit === 'day') {
        calender_in_seconds = calendarValue * 86400;
    }
    else if (calendarUnit === 'week') {
        calender_in_seconds = calendarValue * 604800;
    }
    else if (calendarUnit === 'month') {
        calender_in_seconds = calendarValue * 2592000;
    }
    else if (calendarUnit === 'year') {
        calender_in_seconds = calendarValue * 31536000;
    }
    else{
        console.error("Unrecognized calendar unit.");
    }

    var interval_multiplier = .02; // 2%
    var new_calendar_interval_in_seconds = calender_in_seconds * interval_multiplier;

    if (new_calendar_interval_in_seconds < 60){
        return "ts_1sec";
    }
    else if (new_calendar_interval_in_seconds < 900){
        return "ts_1min";
    }
    else if (new_calendar_interval_in_seconds < 3600){
        return "ts_15min";
    }
    else if (new_calendar_interval_in_seconds < 86400){
        return "ts_1hour";
    }
    else if (new_calendar_interval_in_seconds < 2592000){
        return "ts_1month";
    }
    else if (new_calendar_interval_in_seconds < 0){
        return "ts_1month";
    }
    else{
        console.error("Invalid state reached.");
    }
}

function formatPayloadToSend(body, id_field_name, id_field_value) {
    var moment = require('moment');

    var timeSeriesEntry;
    var unixTimeStamp;
    var jsonBody = JSON.parse(body);
    var arrayLength = jsonBody.length;
    for (var i = 0; i < arrayLength; i++) {
        timeSeriesEntry = jsonBody[i];
        unixTimeStamp = timeSeriesEntry['timestamp']['$date'];

        timeSeriesEntry['timestamp'] = moment(unixTimeStamp).format("YYYY-MM-DD HH:mm:ss");
        jsonBody[i] = timeSeriesEntry;
    }
    var json_body_with_id = {};
    json_body_with_id[id_field_name] = id_field_value;
    json_body_with_id["d"] = jsonBody;
    return json_body_with_id;
}