**This library was initially based on https://www.npmjs.com/package/node-thrift-hbase but 
due to that library's abandonment by the author we had to republish it with our contributions.**

A performant, simple, pooled, cached and promisified HBase client library for NodeJS.

# Working HBase-Thrift compiler combinations
The code supplied here used Thrift 0.9.3 to generate code for HBase 0.98.4.
If you'd like to use this library with different versions, download the desired HBase thrift definition file and compile it using the Thrift compiler of your choice into the project's `gen-nodejs` folder.
If you are successfully working with different HBase/Thrift compiler versions please tell us and we'll add the info here.


- **HBase:** 0.98.4 **Thrift:** 0.9.3

# API 

## Instantiating the HBase client
```javascript
const config = {
    hosts: ["master"],
    port: "9090",
};

const HBase = require('node-thrift2-hbase')(config);
```

## Get

```javascript
var get = HBase.Get('row1');    //row1 is rowKey
get.addFamily('cf');
// get.add('cf'); identical to addFamily

get.addColumn('info', 'name');
// get.add('info', 'name'); identical to addColumn

get.addTimestamp('info', 'name', 1414385447707);
// get.add('info', 'name', 1414385447707); identical to addTimestamp

get.setMaxVersions(3);

//last ten days as timerange
get.setTimeRange({
    minStamp: Date.now() - 10 * 24 * 60 * 60 * 1000,
    maxStamp: Date.now()
});

HBase.getAsync('users', get)
    .then(function (data) {
        console.log("Data for user with key 'row1':");
        console.log('==============================');
        _.each(data[0].columnValues, function (colVal, index) {
            console.log('Column value #', index);
            console.log('family:', colVal.family.toString());
            console.log('qualifier:', colVal.qualifier.toString());
            console.log('value:', colVal.value.readInt32BE(0, 4));
        });
    })
    .catch(function (err) {
        console.log('error:', err);
    });

HBase.get('users', get, function (err, data) { //get users table
    if (err) {
        console.log('error:', err);
        return;
    }

    console.log("Data for user with key 'row1':");
    console.log('==============================');
    _.each(data[0].columnValues, function (colVal, index) {
        console.log('Column value #', index);
        console.log('family:', colVal.family.toString());
        console.log('qualifier:', colVal.qualifier.toString());
        console.log('value:', colVal.value.readInt32BE(0, 4));
    });
});
```
A shorthand version is the `getRow` function:
```javascript
HBase.getRow('users', 'row1', ['info:name', 'ecf'], 1,
    function (err, data) {
        if (err) {
            console.log('error:', err);
            return;
        }
        console.log("Data for user with key 'row1':");
        console.log('==============================');
        _.each(data[0].columnValues, function (colVal, index) {
            console.log('Column value #', index);
            console.log('family:', colVal.family.toString());
            console.log('qualifier:', colVal.qualifier.toString());
            console.log('value:', colVal.value.readInt32BE(0, 4));
        });
    });

HBase.getRowAsync('users', 'row1', ['info:name', 'ecf'], 1)
    .then(function (data) {
        console.log("Data for user with key 'row1':");
        console.log('==============================');
        _.each(data[0].columnValues, function (colVal, index) {
            console.log('Column value #', index);
            console.log('family:', colVal.family.toString());
            console.log('qualifier:', colVal.qualifier.toString());
            console.log('value:', colVal.value.readInt32BE(0, 4));
        });
    })
    .catch(function (err) {
        console.log('error:', err);
    });
```

## Use put or putRow function to insert or update data
<br>

##put( table, put, callback)##
<br>

```javascript
var put = hbaseClient.Put('row1');    //row1 is rowKey

put.add('info','click','100'); // 100 must be string
put.add('info','click',{value:100,type:'integer'}); // to write as Int32BE buffer
put.add('info','click',{value:10.5,type:'float'}); // to write as FloatBE buffer

put.add('info','name','beijing',new Date().getTime());

put.add('ecf','name','zhudaxian');

hbaseClient.put('users',put,function(err){ //put users table

    if(err){

        console.log('error:',err);

        return;

    }

    console.log(err,'put is successfully');

});

```
* //info and ecf are family

* //click and name is qualifier

* //beijing is value

* timestamp is now Date() and this value also by coustom

##putRow(table,row,columns,value,callback)##

```javascript

hbaseClient.putRow('users','row1','info:name','phoneqq.com',function(err){ 
    //put users table
    
    if(err){
        console.log('error:',err);
        return;
    }
    
    console.log(err,'put is successfully');
});

```

##putRow(table,row,columns,value,timestamp,callback)##

```javascript

hbaseClient.putRow('users','row1','info:name','phoneqq.com',1414140874929,function(err){ 
    //put users table
    
    if(err){
        console.log('error:',err);
        return;
    }
    
    console.log(err,'put is successfully');
});

```
* //'users' is table name

* //row1 is rowKey

* //'info:name' is right. info is family, name is qualifier

* //function is callback function

* //phoneqq.com is value
 
* //1414140874929 is timestamp ,not must,if not so auto generate new Date()

<br>
#4 . Use inc or incRow function to update data
<br>

##inc( table, inc, callback)##
<br>

```javascript

var inc = hbaseClient.Inc('row1');    //row1 is rowKey

inc.add('info','counter');

inc.add('info','counter2');

hbaseClient.inc('users',inc,function(err,data){ 
    //inc users table

    if(err){
        console.log('error:',err);
        return;
    }

    console.log(err,data);

});

```

##incRow( table, rowKey, family:qualifier, callback)##
<br>

```javascript

hbaseClient.incRow('users','row1','info:counter',function(err,data){ //inc users table

    if(err){
        console.log('error:',err);
        return;
    }

    console.log(err,data);
    //data is return new counter object
});

```

<br>
#5 . Use del or delRow function to delete data
<br>

##del( table, del, callback)##

```javascript

var del = hbaseClient.Del('row1');    //row1 is rowKey

//del.addFamily('ips');   //delete family ips
//del.addColumn('info','click2'); //delete family and qualifier info:click2
//del.addTimestamp('info','click3',1414136046864); //delete info:click3 and timestamp

//or Recommend this function add

del.add('info');    //delete all family info
del.add('info','name');   //delete family and qualifier info:name
del.add('info','tel',1414136046864); //delete info:tel and timestamp

del.add('ecf'); //delete other family ecf
del.add('ecf','name');  //delete family and qualifier ecf:name
del.add('ecf','tel',1414136119207); //delete info:tel and timestamp

//del.add('ips'); //is error ,because this family ips is not exist

hbaseClient.del('users',del,function(err){ //put users table
    if(err){
        console.log('error:',err);
        return;
    }
    console.log(err,'del is successfully');
});

```

##delRow( table, rowKey, family:qualifier, callback)##
<br>

```javascript

hbaseClient.delRow('users','row1','info:name',function(err){ 
    //put users table
    
    if(err){
        console.log('error:',err);
        return;
    }
    
    console.log(err,'del is successfully');
});

```

##delRow( table, rowKey, family:qualifier, timestamp, callback)##
<br>

```javascript

hbaseClient.delRow('users','row1','info:name',1414137991649,function(err){ 
    //put users table
    
    if(err){
        console.log('error:',err);
        return;
    }
    
    console.log(err,'del is successfully');
});

```

<br>
#6 . Use scan or scanRow function to query data
<br>

```javascript

var scan = hbaseClient.Scan();

//get.addFamily('cf');  //add not found column is error

//scan.addFamily('info');  //add all family

//scan.addStartRow('row1');   //start rowKey

//scan.addStopRow('row1p');   //stop rowKey

//scan.addColumn('info','name');  //add family and qualifier

//scan.addColumn('ecf','name');   //add other family

//scan.setMaxVersions(1); //set maxversions

//scan.addNumRows(10); //search how much number rows

//or Recommend this function add

scan.addStartRow('row1');   //start rowKey

scan.addStopRow('row1p');   //stop rowKey

scan.add('info');    //scan all family info

scan.add('info','name');   //scan family and qualifier info:name

scan.add('ecf'); //scan other family ecf

scan.add('ecf','name');  //scan family and qualifier ecf:name

scan.setMaxVersions(1); //set maxversions

scan.addNumRows(10); //search how much number rows

hbaseClient.scan('users',scan,function(err,data){ //get users table
    if(err){
        console.log('error:',err);
        return;
    }
    console.log(err,data);

//    console.log(err,data[0].columnValues);
});

```

##scanRow(table,startRow,stopRow,columns,numRows,callback)##
<br>

 * //table is search tableName,must 
 
 * //startRow is first rowKey,must
 
 * //stopRow is end rowKey,must
 
 * //columns is family or family and qualifier,is not must
   //example : ['info:name','ecf']

 * //numRows is count rows, is not must,if none the default is 10.
 
 * //callback is function
 
##scanRow(table,startRow,stopRow,callback)##

```javascript

hbaseClient.scanRow('users','row1','row1b',function(err,data){ 
    //get users table
    
    if(err){
        console.log('error:',err);
        return;
    }
    
    console.log(err,data);
});

```


 ##scanRow(table,startRow,stopRow,colmuns,callback)##


```javascript

hbaseClient.scanRow('users','row1','row1b',['info:name','ecf'],function(err,data){ 
    //get users table
    
    if(err){
        console.log('error:',err);
        return;
    }
    
    console.log(err,data);
});

```

##scanRow(table,startRow,stopRow,columns,numRows,callback)##

```javascript

hbaseClient.scanRow('users','row1','row1b',['info:name','ecf'],10,function(err,data){ 
    //get users table
    
    if(err){
        console.log('error:',err);
        return;
    }
    
    console.log(err,data);
});

```

<br>
<br>

# Table Salting
What is "salting"? The term is taken from the encryption nomenclature, but for our purposes it just means adding a predictable string to a key. The way HBase stores rows means that if the keys are not spread across the string spectrum, then the data will physically be kept in a "not spread" manner - for example, having most rows of a table on very few `Region Server`s. So if your keys are well-spread, so is your data. This allows for faster and more parallel reads/writes en-masse. The only problem is keeping track of which table has its keys salted, and exactly how were the keys salted. We have a solution for that:

```javascript
var hbase = require('node-thrift2-hbase')(hbaseConfig);
hbase.saltMap = {
    'myTable1': hbase.saltFunctions.saltByLastKeyCharCode,
    'myTable2': hbase.saltFunctions.saltByLastKeyCharCode
};
```

All `get` and `put` operations for tables specified in the `saltMap` will be 
salted using the given function. `hbase.saltFunctions` contains some ready-made salt functions. If you have a salt function you find useful, don't hesitate to make a PR adding it!
