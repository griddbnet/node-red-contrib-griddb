var griddb = require('griddb_node');

module.exports = function(RED) {
    let factory = griddb.StoreFactory.getInstance();
    let store = factory.getStore({
        "host": '239.0.0.1',
        "port": 31999,
        "clusterName": "defaultCluster",
        "username": "admin",
        "password": "admin"
    });

    function GridDBQuery(config) {
        RED.nodes.createNode(this,config);
        var node = this; 
        var name = config.name;
        var tql = config.tql;
 
        node.on('input', function(msg) { 

            let localname = name;
            let localtql = tql;
            if(msg.name != null && msg.name != "")
                localname = msg.name;

            if(msg.tql != null && msg.tql != "")
                localtql = msg.tql;

            var result = []
            store.getContainer(localname)
                .then(cont => {
                    q = cont.query(localtql)
                    return q.fetch();
                })
            	.then(rs => {
            		while (rs.hasNext()) {
                        result.push(rs.next())
            		}
                console.log(result)
                msg.payload = result
                node.send(msg);
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
            console.log(msg)
        });

    }
    console.log("Registering griddb-query")
    RED.nodes.registerType("griddb-query",GridDBQuery);
}
