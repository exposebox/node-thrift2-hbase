/**
 * Created by rubinus on 14-10-20.
 */
var HBase = require('../');

var config = {
    host: 'master',
    port: 9090
};

var hbaseClient = HBase.client(config);

var put = hbaseClient.Put('row1');    //row1 is rowKey

put.add('info','click','100'); // 100 must be string

put.add('info','name','beijing',new Date().getTime());

put.add('ecf','name','zhudaxian');

hbaseClient.put('users',put,function(err){ //put users table
    if(err){
        console.log('error:',err);
        return;
    }
    console.log(err,'put is successfully');
});

