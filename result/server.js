/*
 Nodejs results viewer reads data from a relational database
 The relational database can be either DB2 or Postgres
 In the case of DB2, the method of connection can be either ODBC or REST
 The behavior is configurable via environment variable settings

 Following are valid database-related environment variable scenarios

 For Postgres
 Variable name     Desc
 -------------     -----
 WHICH_DBM         Has to be exactly "POSTGRES"
 PG_CONNECT_STRING A Postgress connection string. Resembling RFC 1738 syntax.  (default: postgres://admin:admin@postgresql/db)

 For DB2 via ODBC
 Variable name     Desc
 -------------     -----
 WHICH_DBM         Has to be exactly "DB2"
 DB2_METHOD        Has to be exactly "ODBC"
 DB2_DRIVER        Driver name (default: "IBM DB2 ODBC DRIVER")
 DB2_DATABASE      Database name (default: "db2sample")
 DB2_HOSTNAME      Hostname or ip address (default: "localhost")
 DB2_PORT          Port number (default: "50000")
 DB2_PROTOCOL      Protocol the driver should use (default: "TCPIP")
 DB2_SCHEMA        Database table schema name (default: "team1")
 DB2_USER          Database user (default: "db2inst1")
 DB2_PASSWORD      Database password (default: "passw0rd")
 
 For DB2 via REST  (only basic auth is supported)
 Variable name     Desc
 -------------     -----
 WHICH_DBM         Has to be exactly "DB2"
 DB2_METHOD        Has to be exactly "REST"
 DB2_REST_APIURL   The Rest API URL - For example http://9.60.87.146:countVotes (no default)
 DB2_USER          Database user (no default)
 DB2_PASSWORD      Database password (no default)

*/

var express = require('express'),
    async = require('async'),
    path = require('path'),
    cookieParser = require('cookie-parser'),
    bodyParser = require('body-parser'),
    methodOverride = require('method-override'),
    app = express(),
    server = require('http').Server(app),
    io = require('socket.io')(server),
    needle = require('needle'),
    util = require('util');

var port = process.env.PORT || 8080;

io.set('transports', ['polling']);

io.sockets.on('connection', function (socket) {
    socket.emit('message', { text : 'Welcome!' });
    socket.on('subscribe', function (data) {
    socket.join(data.channel);
    });
});

var which_dbm = process.env.WHICH_DBM;
console.log ("WHICH_DBM=" + which_dbm);

if (which_dbm.includes("POSTGRES")) {
    console.log("Connecting to Postgres");
    var pg = require('pg');
    var { Pool } = require('pg');
    // example environment variable settings
    //   PG_CONNECT_STRING='postgres://admin:admin@postgresql.voting-app-demo-db2.svc.cluster.local/db'
    //   PG_CONNECT_STRING='postgres://admin:admin@postgresql/db'
    var pgconnectstr = process.env.PG_CONNECT_STRING || 'postgresq://admin:admin@postresql/db';
    var pool = new pg.Pool({connectionString: pgconnectstr});

    async.retry(
        {times: 1000, interval: 1000},
        function(callback) {
        pool.connect(function(err, client, done) {
            if (err) {
            console.error("Waiting for db");
            console.log("pg error code:", err.code);
            }
            callback(err, client);
        });
        },
        function(err, client) {
        if (err) {
            return console.error("Giving up");
        }
        console.log("Connected to Postgres database");
        getVotes_pg(client);
        }
    );
} else if (which_dbm.includes("DB2")) {
    if (process.env.DB2_METHOD == "ODBC") {
        console.log("Connecting to DB2 database via ODBC");
        var Pool = require("ibm_db").Pool;
        var pool = new Pool();
        var db2connectstr = util.format('DATABASE=%s;HOSTNAME=%s;PORT=%s;UID=%s;PWD=%s;PROTOCOL=%s',
            process.env.DB2_DATABASE || 'db2sample',
            process.env.DB2_HOSTNAME || 'localhost',
            process.env.DB2_PORT     || '50000',
            process.env.DB2_USER     || 'db2inst1',
            process.env.DB2_PASSWORD || 'passw0rd',
            process.env.DB2_PROTOCOL || 'TCPIP'
        );
        var db2_schema = process.env.DB2_SCHEMA || 'team1'

        async.retry(
            {times: 1000, interval: 1000},
            function(callback) {
                pool.open(db2connectstr, function (err, db) {
                    if (err) {
                        console.error("Waiting for db");
                        console.log("db error code:", err);
                    }
                    callback(err, db);
                });
            },
            function(err, db) {
            if (err) {
                return console.error("Giving up");
            }
            console.log("Connected to DB2 database");
            getVotes_db2_odbc(db);
            }
        );
    } else if (process.env.DB2_METHOD == "REST") {
        console.log("Connecting to DB2 database via REST");
        var resturl = process.env.DB2_REST_APIURL;
        getVotes_db2_rest(resturl)
    } else {
        console.log("'DB2_METHOD' environment variable is invalid.  Should be 'ODBC' or 'REST'");
    }
} else {
    console.log("'WHICH_DBM' environment variable is invalid.  Should be 'POSTGRES' or 'DB2'");
};

function getVotes_pg(client) {
    client.query('SELECT vote, COUNT(id) AS count FROM votes GROUP BY vote order by vote', [], function(err, result) {
        if (err) {
          console.error("Error performing query: " + err);
        } else {
          var votes = collectVotesFromResult_pg(result);
          io.sockets.emit("scores", JSON.stringify(votes));
        }
    
        setTimeout(function() {getVotes_pg(client) }, 1000);
      });
}

function collectVotesFromResult_pg(result) {
    var votes = {a: 0, b: 0};

    result.rows.forEach(function (row) {
      votes[row.vote] = parseInt(row.count);
    });
  
    return votes;
}

function getVotes_db2_odbc(db) {
    var select_stmt = util.format('SELECT vote, COUNT(id) AS count FROM %s.votes GROUP BY vote order by vote', db2_schema);
    db.query(select_stmt, [], function(err, rows, sqlca) {
      if (err) {
        console.error("Error performing query: " + err + " , SQLCA: " + sqlca);
      } else {
        var votes = collectVotesFromResult_db2(rows);
        io.sockets.emit("scores", JSON.stringify(votes));
      }
  
      setTimeout(function() {getVotes_db2_odbc(db) }, 1000);
    });
}

// Upon entry, rows will contain a JSON array that looks like;
//    [ { VOTE: 'a', COUNT: 4 }, { VOTE: 'b', COUNT: 2 } ]
function collectVotesFromResult_db2(rows) {
    var votes = {a: 0, b: 0};

    //console.log(rows);

    rows.forEach(function (row) {
    votes[row.VOTE] = parseInt(row.COUNT);
    });

    return votes;
}

function getVotes_db2_rest(resturl) {
    var options = {
        auth: 'basic',
        username: process.env.DB2_USER,
        password: process.env.DB2_PASSWORD,
        accept: '*/*',
        content_type: 'application/json'
    };

    needle.get(resturl, options, function(error, response) {
      if (!error && response.statusCode == 200) {
        // response.body will have a json payload containing a single key-value pair {message: 'stuff'}
        // Notice that the message value is a single piece of stringified json.  Here is a real world example;
        // {  message: '{"ResultSet Output":[{"VOTE":"a","COUNT":4},{"VOTE":"b","COUNT":2}],"StatusCode":200,"StatusDescription":"Execution Successful"}' }
        var rows = JSON.parse(response.body.message)["ResultSet Output"];
        // At this point the rows variable will contain json something like
        //   [ { VOTE: 'a', COUNT: 4 }, { VOTE: 'b', COUNT: 2 } ]
        //console.log(rows);
        var votes = collectVotesFromResult_db2(rows);
        io.sockets.emit("scores", JSON.stringify(votes));
      } else {
        console.error("Error calling DB2 REST URL: ", resturl);
        if (error) {
          console.error("error: ", error.message);
        } else {
          console.error("statusCode: ", response.statusCode);
        };
      };
    });

    setTimeout(function() {getVotes_db2_rest(resturl) }, 5000);
}

app.use(cookieParser());
app.use(bodyParser());
app.use(methodOverride('X-HTTP-Method-Override'));
app.use(function(req, res, next) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    res.header("Access-Control-Allow-Methods", "PUT, GET, POST, DELETE, OPTIONS");
    next();
});

app.use(express.static(__dirname + '/views'));

app.get('/', function (req, res) {
    res.sendFile(path.resolve(__dirname + '/views/index.html'));
});

server.timeout = 0;
server.listen(port, function () {
    var port = server.address().port;
    console.log('App running on port ' + port);
});
