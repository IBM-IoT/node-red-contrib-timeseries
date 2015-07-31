/**
  Copyright 2015 IBM Corp.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
**/

module.exports = function(RED) {

  var util = require( "util" );
  var request = require( "request" );
  var moment = require( "moment" );

  function TimeSeriesSimple( config )
  {
    this.buildQueryUrl = function( query ) {
      return this.sqlBaseUrl + '{"$sql":"' + query + '"}';
    };

    RED.nodes.createNode( this, config );

    var self = this;
    this.timeseries = config.timeseries;
    this.table = config.table;
    this.idcolumn = config.idcolumn;
    this.tscolumn = config.tscolumn;
    this.filter_id = config.filter_id;
    this.end_type = config.end_type;
    this.end_ts = config.end_ts;
    this.start_value = config.start_value;
    this.start_unit = config.start_unit;
    this.timeseriesConfig = RED.nodes.getNode( this.timeseries );

    this.sqlBaseUrl = "http://" + this.timeseriesConfig.hostname + ":" + this.timeseriesConfig.port + "/" + this.timeseriesConfig.db + "/system.sql?query=";

    this.tsRowType = null;
    var rowTypeQueryUrl = this.buildQueryUrl( "select name from sysxtdtypes where extended_id=(" +
                                              "select xtd_type_id from sysattrtypes where levelno=1 and extended_id=(" +
                                              "select extended_id from systables,syscolumns where systables.tabid = syscolumns.tabid and " +
                                              "tabname='" + this.table + "' and colname='" + this.tscolumn + "'))" );

    request( rowTypeQueryUrl , function( error , response , body ) {
      if( !error )
      {
        try
        {
          var data = JSON.parse( body );
          if( data.length < 1 ) throw "Row Type data empty.";
          if( typeof data[0] !== "object" || !data[0].hasOwnProperty( "name" ) || typeof data[0].name !== "string" ) throw "Unexpected Row Type data.";
          self.tsRowType = data[0].name;
          self.log( "Row Type: " + self.tsRowType );
        }
        catch( e )
        {
          self.error( e );
        }
      }
      else
      {
        self.error( error );
      }
    } );

    this.on( "input" , function( msg ) {

      // Don't know the row type yet, for some reason
      if( self.tsRowType === null ) return;

      // Make checks simpler...
      msg.payload = msg.payload || {};

      var filterID = self.filter_id;
      if( msg.payload.hasOwnProperty( self.idcolumn ) )
      {
        filterID = msg.payload[ self.idcolumn ];
      }

      if( !filterID )
      {
        self.error( "Missing ID. Either manually configure one or supply one through msg.payload." + self.idcolumn );
        return;
      }

      var momentFormat = "YYYY-MM-DD HH:mm:ss";
      var momentStart, momentEnd;

      if( msg.payload.hasOwnProperty( "end" ) )
      {
        momentEnd = moment( msg.payload.end );
      }
      else
      {
        momentEnd = ( this.end_type === "now" ? moment() : moment( this.end_ts ) );
      }

      if( msg.payload.hasOwnProperty( "start" ) )
      {
        momentStart = moment( msg.payload.start );
      }
      else
      {
        momentStart = moment( momentEnd ).subtract( this.start_value , this.start_unit );
      }

      // Flip, if necessary
      if( !momentStart.isBefore( momentEnd ) )
      {
        var temp = momentStart;
        momentStart = momentEnd;
        momentEnd = temp;
      }

      var queryUrl = self.buildQueryUrl( "select t.* from table(transpose(" +
                                         "'select " + self.tscolumn + " from " + self.table + " where " + self.idcolumn + "=" + filterID + "',NULL::" + self.tsRowType +
                                         ",'" + momentStart.format( momentFormat ) + "'::datetime year to fraction(5)" +
                                         ",'" + momentEnd.format( momentFormat ) + "'::datetime year to fraction(5))) as tab(t)" );

      request( queryUrl , function( error , response , body ) {
        if( !error )
        {
          try
          {
            var data = JSON.parse( body );
            self.send( { payload : data } );
          }
          catch( e )
          {
            self.error( e );
          }
        }
        else
        {
          self.error( error );
        }
      } );
    } );
  }

  RED.nodes.registerType( "timeseries-- in", TimeSeriesSimple );

  function TimeSeriesSimpleOut( config )
  {
    RED.nodes.createNode( this , config );

    var self = this;
    this.table = config.table;
    this.timeseriesConfig = RED.nodes.getNode( config.timeseries );
    this.uri = "http://" + this.timeseriesConfig.hostname + ":" + this.timeseriesConfig.port + "/" + this.timeseriesConfig.db + "/" + this.table;

    this.on( "input" , function( msg ) {

      if( !msg.hasOwnProperty( "payload" ) )
      {
        self.error( "Missing payload." );
        return;
      }

      var options = {
        uri : this.uri,
        headers : {
          "Content-Type" : "application/json"
        },
        method : "POST",
        body : JSON.stringify( msg.payload )
      };

      request( options , function( error , response , body ) {
        if( error || response.statusCode != 200 )
        {
          if( error )
          {
            self.error( error );
          }
          else
          {
            self.error( "(" + response.statusCode + ") " + body );
          }
        }
      } );

    } );
  }

  RED.nodes.registerType( "timeseries-- out", TimeSeriesSimpleOut );
};
