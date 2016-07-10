/**
 * Created by rubinus on 14-10-20.
 */
var HBase = require('../');

var config = {
    host: 'master',
    port: 9090
};

var hbaseClient = HBase.client(config);

var scan = hbaseClient.Scan();

//get.addFamily('cf');  //add not found column is error

//scan.addFamily('info');  //add all family
//
scan.setStartRow('row1');   //start rowKey
//
scan.setStopRow('row1p');   //stop rowKey
//
//scan.addColumn('info','name');  //add family and qualifier
//
//scan.addColumn('ecf','name');   //add other family
//
scan.setMaxVersions(2); //set maxversions

scan.setLimit(10); //search how much number rows


//or Recommend this function add

scan.add('info');    //scan all family info
scan.add('info','name');   //scan family and qualifier info:name

scan.add('ecf'); //scan other family ecf
scan.add('ecf','name');  //scan family and qualifier ecf:name


hbaseClient.scan('users',scan,function(err,data){ //get users table
    if(err){
        console.log('error:',err);
        return;
    }
    console.log(err,data);

    console.log(err,data[0].columnValues);
});


//already run this command

//thrift --gen js:node /install/hbase-0.98.5/hbase-thrift/src/main/resources/org/apache/hadoop/hbase/thrift2/hbase.thrift

