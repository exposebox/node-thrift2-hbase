/**
 * Created by rubinus on 14-10-20.
 */
var HBase = require('../');

var config = {
    host: 'master',
    port: 9090
};

var hbaseClient = HBase.client(config);

hbaseClient.putRow('users','row1','info:name','phoneqq.com',1414140874929,function(err){ //put users table
    if(err){
        console.log('error:',err);
        return;
    }
    console.log(err,'put is successfully');
});

