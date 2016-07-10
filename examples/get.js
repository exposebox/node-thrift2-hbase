/**
 * Created by rubinus on 14-10-20.
 */
var HBase = require('../');

var config = {
    host: 'master',
    port: 9090
};

var hbaseClient = HBase.client(config);

var get = hbaseClient.Get('row1');    //row1 is rowKey
//get.addFamily('cf');  //add not found column is error
//get.addFamily('info');
//get.addColumn('info','name');   //this replace addFamily
//get.addTimestamp('info','name',1414385447707);
//get.addColumn('ecf','name');
//get.setMaxVersions(3);

//or Recommend this function add

get.add('info');    //get all family info
get.add('info','name');   //get family and qualifier info:name
get.add('info','name',1414385447707); //get info:name and timestamp

get.add('ecf'); //get other family ecf
get.add('ecf','name');  //get family and qualifier ecf:name
get.add('ecf','name',1414385555890); //get info:name and timestamp


hbaseClient.get('users',get,function(err,data){ //get users table
    if(err){
        console.log('error:',err);
        return;
    }
    console.log(err,data);

//    console.log(err,data.columnValues[0].value);
});


//already run this command

//thrift --gen js:node /install/hbase-0.98.5/hbase-thrift/src/main/resources/org/apache/hadoop/hbase/thrift2/hbase.thrift

