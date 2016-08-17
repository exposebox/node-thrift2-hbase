**This library was initially based on https://www.npmjs.com/package/node-thrift-hbase but 
due to that library's abandonment by the author we had to republish it with our contributions.**
<br>
**The library runs in our production Real-Time-Bidder right now with very good performance. Please open issues! PRs welcome!**
<br>


#Use thrift2 to CRUD for hbase
Now with support for table salting!
```javascript
var hbase = require('node-thrift2-hbase')(hbaseConfig);
hbase.saltMap = {
    'model:model_output': hbase.saltFunctions.saltByLastKeyCharCode,
    'stats:user_product_views': hbase.saltFunctions.saltByLastKeyCharCode
};
```

All `get` and `put` operations for tables specified in the `saltMap` will be 
salted using the given function.

Compiled using Thrift 0.9.3 for HBase version 0.98.4
<br>

##1 . create Hbase instance client

```javascript
var HBase = require('node-thrift2-hbase');

var config = {
    host: ['host1','host2'],
    port: 9090,
    timeout:1000
};

var hbaseService = HBase(config);
var hbasePool = hbaseService.clientPool;
//acquire client to HBase
hbasePool.acquire(function (err, hbaseClient) {
    if(err)
        console.log('error:',err);
    hbaseClient.getRow('users','row1',['info:name','ecf'],1,function(err,data){ //get users table
        if(err){
            console.log('error:',err);
            //destroy client on error
            hbasePool.destroy(hbaseClient);
            return;
        }
        //release client in the end of use.
        hbasePool.release(hbaseClient);
        console.log(err,data);
    });

});

```
#2 . Use get or getRow function to query data

##get(table,get,callback)
<br>

```javascript
var get = hbaseClient.Get('row1');    //row1 is rowKey

//get.addFamily('cf');  //add not found column is error

//get.addFamily('info');

//get.addColumn('info','name');   //this replace addFamily

//get.addTimestamp('info','name',1414385447707);

//get.addColumn('ecf','name');

//get.setMaxVersions(3);  //default 1

//or Recommend this function add

get.add('info');    //get all family info

get.add('info','name');   //get family and qualifier info:name

get.add('info','name',1414385447707); //get info:name and timestamp

get.add('ecf'); //get other family ecf

get.add('ecf','name');  //get family and qualifier ecf:name

get.add('ecf','name',1414385555890); //get info:name and timestamp


hbaseClient.get('users',get,function(err,data){ 
    //get users table

    if(err){
        console.log('error:',err);
        return;
    }
    
    console.log(err,data);

});

```

##getRow(table,rowKey,columns,versions,callback)##
<br>

###introduce getRow function###
* hbaseClient.getRow = function (table,rowKey,columns,versions,callback) { 

    * //table is must
    * //rowKey is must
    * //columns is not must,the default is get all row value
    * //versions is not must, the default is 1 ,if have this params,string is auto cost number
* }

------
###getRow( table, rowKey, callback)###

```javascript
hbaseService.getRow('users','row1',function(err,data){
    //get users table

    if(err){
        console.log('error:',err);
        return;
    }

    console.log(err,data);

});

```

----

###getRow( table, rowKey, columns, callback)###

```javascript

hbaseClient.getRow('users','row1',['info:name','ecf'],function(err,data){ 
    //get users table

    if(err){
        console.log('error:',err);
        return;
    }

    console.log(err,data);

});

```

----

###getRow( table, rowKey, columns, versions, callback)###


```javascript

hbaseClient.getRow('users','row1',['info:name','ecf'], 2 ,function(err,data){ 
    //get users table

    if(err){
        console.log('error:',err);
        return;
    }

    console.log(err,data);

});

```

* //'users' is table name

* //row1 is rowKey

* //[] is family or family qualifier

* //['info:name','info:tel'] is right. info is family, name and tel is qualifier

* //['info:name','ecf'] is rigth too, info is family , ecf is family

* //function is callback function

* //2 is Maxversion ,default is 1

<br>
#3 . Use put or putRow function to insert or update data
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
