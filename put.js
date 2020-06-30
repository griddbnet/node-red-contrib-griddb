var griddb = require('griddb_node');

var COLUMNTYPES = {'STRING': 0, 'BOOL': 1, 'BYTE' : 2, 'SHORT': 3, 'INTEGER' : 4, 'LONG' : 4, 'FLOAT' : 6, 'DOUBLE': 7, 'TIMESTAMP': 8 }
module.exports = function(RED) {
    let factory = griddb.StoreFactory.getInstance();
    let store = factory.getStore({
        "host": '239.0.0.1',
        "port": 31999,
        "clusterName": "defaultCluster",
        "username": "admin",
        "password": "admin"
    });

    function GridDBPut(config) {
        RED.nodes.createNode(this,config);
        var node = this; 
        var name = config.name;

        var ctype = griddb.ContainerType.COLLECTION;
        var rowkey = true;
        
        if (config.ctype == "TIME_SERIES")
            ctype = griddb.ContainerType.TIME_SERIES;

        var columnlist = [];
        console.log(config.columnlist)
        var column = JSON.parse(config.columnlist)  
        for(i=0; i<column.length;i++) {
            console.log(column[i])
            columnlist.push([column[i][0],COLUMNTYPES[column[i][1]]])
        }

        console.log(columnlist) 
        if (config.rowkey == "false")
            rowkey = false;

        node.on('input', function(msg) { 

            let localname = name;
            if(msg.topic != null && msg.topic != "")
                localname = msg.topic;

            var colInfo = new griddb.ContainerInfo({
                'name' : localname,
                'columnInfoList' : columnlist,
                'type': ctype, 'rowKey': rowkey
            });
            store.putContainer(colInfo, false)
                .then(cont => {
                    cont.setAutoCommit(false)
                    cont.put(msg.payload)
                    cont.commit()
                    console.log(localname + "="+ msg.payload)
                })
            .catch(err => {
                if (err.constructor.name == "GSException") {
                    for (var i = 0; i < err.getErrorStackSize(); i++) {
                        console.log("[", i, "]");
                        console.log(err.getErrorCode(i));
                        console.log(err.getMessage(i));
                    }
                } else {
                    console.log(err);
                }
            });
//            console.log(msg)
        });

    }
    console.log("Registering griddb-put")
    RED.nodes.registerType("griddb-put",GridDBPut);
}
