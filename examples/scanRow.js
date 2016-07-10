/**
 * Created by rubinus on 14-10-20.
 */
var HBase = require('../');

var config = {
    host: 'master',
    port: 9090
};

var hbaseClient = HBase.client(config);

hbaseClient.scanRow('users','row1','row1b',['info:name','ecf'],10,function(err,data){ //get users table
    if(err){
        console.log('error:',err);
        return;
    }
    console.log(err,data);
});

