#!/usr/bin/env node
var pull = require("pull-stream");
var debug = require("debug");
var log = debug("pando-computing");
var logMonitoring = debug("pando-computing:monitoring");
var logMonitoringChildren = debug("pando-computing:monitoring:children");
var logHeartbeat = debug("pando-computing:heartbeat");
var parse = require("../src/parse.js");
var bundle = require("../src/bundle.js");
var electronWebRTC = require("electron-webrtc");
var createProcessor = require("../src/processor.js");
var Node = require("webrtc-tree-overlay");
var Server = require("pando-server");
var BootstrapClient = require("webrtc-bootstrap");
var os = require("os");
var fs = require("fs");
var path = require("path");
var website = require("simple-updatable-website");
var http = require("http");
var WebSocket = require("ws");
var express = require("express");
var probe = require("pull-probe");
var mkdirp = require("mkdirp");
var sync = require("pull-sync");
var toPull = require("stream-to-pull-stream");
var limit = require("pull-limit");
const portfinder = require("portfinder");
var duplexWs = require("pull-ws");
var AWS = require("aws-sdk");

AWS.config.update({
  region: 'ap-southeast-2',
  accessKeyId: 'AKIAV6JUFBZ7HTPRPCRR',
  secretAcesssKey: '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'
})
const dynamoDB = new AWS.DynamoDB.DocumentClient();

// var args = parse(process.argv.slice(2));

var wrtc = electronWebRTC({ headless: true });

function getIPAddresses() {
  var ifaces = os.networkInterfaces();
  var addresses = [];

  Object.keys(ifaces).forEach(function (ifname) {
    var alias = 0;

    ifaces[ifname].forEach(function (iface) {
      if (iface.family !== "IPv4" || iface.internal !== false) {
        // skip over internal (i.e. 127.0.0.1) and non-ipv4 addresses
        return;
      }

      if (alias >= 1) {
        // this single interface has multiple ipv4 addresses
        addresses.push(iface.address);
      } else {
        // this interface has only one ipv4 adress
        addresses.push(iface.address);
      }
    });
  });
  return addresses;
}

// process.stdout.on("error", function (err) {
//   log("process.stdout:error(" + err + ")");
//   if (err.code === "EPIPE") {
//     process.exit(1);
//   }
// });

class Project {
  constructor({
    port,
    module,
    id,
    items = [],
    secret = "INSECURE-SECRET",
    seed = null,
    heartbeat = 30000,
    batchSize = 1,
    degree = 10,
    globalMonitoring = false,
    iceServers = ["stun:stun.l.google.com:19302"],
    reportingInterval = 3,
    bootstrapTimeout = 60,
    syncStdio = false,
  }) {
    this.port = port;
    this.server = null;
    this.processor = null;
    this.host = null;
    this.wsVolunteersStatus = {};
    this.statusSocket = null;
    this.module = path.join(process.cwd(), module);
    this.secret = secret;
    this.seed = seed;
    this.heartbeat = heartbeat;
    this.batchSize = batchSize;
    this.degree = degree;
    this.globalMonitoring = globalMonitoring;
    this.iceServers = iceServers.map((url) => ({ urls: url }));
    this.reportingInterval = reportingInterval;
    this.bootstrapTimeout = bootstrapTimeout;
    this.startIdle = true;
    this.items = pull.values(items.map((x) => String(x)));
    this.syncStdio = syncStdio;
    this.statusSocket = null;
    this.wsVolunteersStatus = {};
    this.id = id;
  
    this.start = () => {
        bundle(this.module, (err, bundlePath) => {
          if (err) {
            console.error(err);
            process.exit(1);
          }

          const _this = this;

          log("creating bootstrap server");
          var publicDir = path.join(__dirname, "../local-server/public");
          mkdirp.sync(publicDir);
          this.server = new Server({
            secret: this.secret,
            publicDir: publicDir,
            port: this.port,
            seed: this.seed,
          });
          this.host = "localhost:" + this.port;

          this.server._bootstrap.upgrade("/volunteer", (ws) => {
            if (this.processor) {
              log("volunteer connected over WebSocket");

              ws.isAlive = true;
              var heartbeat = setInterval(function ping() {
                if (ws.isAlive === false) {
                  logHeartbeat("ws: volunteer connection lost");
                  return ws.terminate();
                }
                ws.isAlive = false;
                ws.ping(function () {});
              }, this.heartbeat);
              ws.addEventListener("close", function () {
                clearInterval(heartbeat);
                heartbeat = null;
              });
              ws.addEventListener("error", function () {
                clearInterval(heartbeat);
                heartbeat = null;
              });
              ws.addEventListener("pong", function () {
                logHeartbeat("ws: volunteer connection pong");
                ws.isAlive = true;
              });

              this.processor.lendStream(function (err, stream) {
                if (err) return log("error lender sub-stream to volunteer: " + err);
                log("lending sub-stream to volunteer");

                pull(
                  stream,
                  probe("volunteer-input"),
                  limit(duplexWs(ws), _this.batchSize),
                  probe("volunteer-output"),
                  stream
                );
              });
            }
          });

          this.server._bootstrap.upgrade("/volunteer-monitoring", (ws) => {
            log("volunteer monitoring connected over WebSocket");

            ws.isAlive = true;
            var heartbeat = setInterval(function ping() {
              if (ws.isAlive === false) {
                logHeartbeat("ws: volunteer monitoring connection lost");
                return ws.terminate();
              }
              ws.isAlive = false;
              ws.ping(function () {});
            }, args.heartbeat);
            ws.addEventListener("close", function () {
              clearInterval(heartbeat);
              heartbeat = null;
            });
            ws.addEventListener("error", function () {
              clearInterval(heartbeat);
              heartbeat = null;
            });
            ws.addEventListener("pong", function () {
              logHeartbeat("ws: volunteer monitoring pong");
              ws.isAlive = true;
            });

            var id = null;
            var lastReportTime = new Date();
            pull(
              duplexWs.source(ws),
              pull.drain(
                function (data) {
                  var info = JSON.parse(data);
                  id = info.id;
                  var time = new Date();
                  this.wsVolunteersStatus[info.id] = {
                    id: info.id,
                    timestamp: time,
                    lastReportInterval: time - lastReportTime,
                    performance: info,
                  };
                  lastReportTime = time;
                },
                function () {
                  if (id) {
                    delete this.wsVolunteersStatus[id];
                  }
                }
              )
            );
          });

          getIPAddresses().forEach((addr) => {
            console.error(
              "Serving volunteer code at http://" + addr + ":" + this.port
            );
          });

          log("Serializing configuration for workers");
          fs.writeFileSync(
            path.join(__dirname, "../public/config.js"),
            "window.pando = { config: " +
              JSON.stringify({
                batchSize: this.batchSize,
                degree: this.degree,
                globalMonitoring: this.globalMonitoring,
                iceServers: this.iceServers,
                reportingInterval: this.reportingInterval * 1000,
                requestTimeoutInMs: this.bootstrapTimeout * 1000,
                version: "1.0.0",
              }) +
              " }"
          );

          log("Uploading files to " + this.host + " with secret " + this.secret);
          website.upload(
            [
              bundlePath,
              path.join(__dirname, "../public/config.js"),
              path.join(__dirname, "../public/index.html"),
              path.join(__dirname, "../public/volunteer.js"),
              path.join(__dirname, "../public/simplewebsocket.min.js"),
              path.join(
                __dirname,
                "../node_modules/bootstrap/dist/css/bootstrap.min.css"
              ),
              path.join(
                __dirname,
                "../node_modules/bootstrap/dist/js/bootstrap.min.js"
              ),
              path.join(__dirname, "../node_modules/jquery/jquery.min.js"),
              path.join(
                __dirname,
                "../node_modules/popper.js/dist/umd/popper.min.js"
              ),
            ],
            this.host,
            this.secret,
            (err) => {
              if (err) throw err;
              log("files uploaded successfully");

              log("connecting to bootstrap server");
              var bootstrap = new BootstrapClient(this.host);

              log("creating root node");
              var root = new Node(bootstrap, {
                requestTimeoutInMs: this.bootstrapTimeout * 1000, // ms
                peerOpts: {
                  wrtc: wrtc,
                  config: { iceServers: this.iceServers },
                },
                maxDegree: this.degree,
              }).becomeRoot(this.secret);

              this.processor = createProcessor(root, {
                batchSize: this.batchSize,
                bundle: !this.startIdle
                  ? require(bundlePath)["/pando/1.0.0"]
                  : function (x, cb) {
                      console.error(
                        "Internal error, bundle should not have been executed"
                      );
                    },
                globalMonitoring: this.globalMonitoring,
                reportingInterval: this.reportingInterval * 1000, // ms
                startProcessing: !this.startIdle,
              });

              this.processor.on("status", function (rootStatus) {
                var volunteers = {};

                // Adding volunteers connected over WebSockets
                for (var id in _this.wsVolunteersStatus) {
                  volunteers[id] = _this.wsVolunteersStatus[id];
                }

                // Adding volunteers connected over WebRTC
                for (var id in rootStatus.children) {
                  volunteers[id] = rootStatus.children[id];
                }

                var status = JSON.stringify({
                  root: rootStatus,
                  volunteers: volunteers,
                  timestamp: new Date(),
                });

                logMonitoring(status);
                logMonitoringChildren(
                  "children nb: " +
                    rootStatus.childrenNb +
                    " leaf nb: " +
                    rootStatus.nbLeafNodes
                );

                if (_this.statusSocket) {
                  log("sending status to monitoring page");
                  _this.statusSocket.send(status);
                }
              });

              const close = () => {
                log("closing");
                if (this.server) {
                  this.server.close();
                }
                if (root) root.close();
                if (bootstrap) bootstrap.close();
                if (wrtc) wrtc.close();
                if (this.processor) this.processor.close();
              }

              const getItemParams = {
                TableName: 'Record',
                Key: {
                  ProjectId: this.id
                }
              };

              function toggleCreate(that) {
                const params = {
                  TableName: 'Record',
                  Item: {
                    ProjectId: that.id,
                    Output: [],
                  },
                };

                  dynamoDB.put(params, (err, data) => {
                    if (err) {
                      console.error('Error putting item to DynamoDB:', err);
                    } else {
                      console.log(`Item put to DynamoDB - ProjectID : ${that.id}`);
                    }
                  });
                }

                dynamoDB.get(getItemParams, (err, data) => {
                  if (err) {
                    console.error('Error retrieving project:', err);
                  } else {
                    const item = data.Item;
                    if (item) {
                      // Item with the specified key exists in the table
                      console.log('Project exists:', item);
                    } else {
                      // Item with the specified key does not exist in the table
                      console.log('Project does not exist. Create new Record');
                      toggleCreate(this)
                    }
                  }
                });

                var io = {
                  source: this.items,
                  sink: pull.drain(
                    (x) => {
                      // Push data to DynamoDB
                      const params = {
                        TableName: 'Record',
                        Key: {
                          ProjectId: this.id,
                        },
                        UpdateExpression: "SET #myOutput = list_append(#myOutput, :newOutput)",
                        ExpressionAttributeNames: {
                          '#myOutput': 'Output'
                        },
                        ExpressionAttributeValues: {
                          ":newOutput": [Number(x)],
                        },
                      };

                      dynamoDB.update(params, (err, data) => {
                        if (err) {
                          console.error('Error putting item to DynamoDB:', err);
                        } else {
                          console.log(`Item put to DynamoDB - ProjectID : ${this.id}`);
                        }
                      })
                    },
                    function (err) {
                      log("drain:done(" + err + ")");
                      if (err) {
                        console.error(err.message);
                        console.error(err);
                        close();
                        process.exit(1);
                      } else {
                        close();
                        process.exit(0);
                      }
                    }
                  ),
                };
                

              pull(
                io,
                pull.through(log),
                probe("pando:input"),
                this.processor,
                probe("pando:result"),
                pull.through(log),
                io
              );
            }
          );
        });
    };
  }
}

portfinder.getPort(function (err, port) {
  if (err) throw err;

  const projectA = new Project({
    port,
    id: "example0",
    module: "examples/square.js",
    items: [1, 2, 3, 4, 5, 6, 7, 8, 9],
  });

  projectA.start();
});

portfinder.getPort(function (err, port) {
  if (err) throw err;

  const projectA = new Project({
    port,
    id: "example1",
    module: "examples/square.js",
    items: [100, 200, 300, 400, 500, 600, 700, 800, 900],
  });

  projectA.start();
});
// bundle(args.module, function (err, bundlePath) {
//   if (err) {
//     console.error(err);
//     process.exit(1);
//   }

//   var statusSocket = null;
//   var wsVolunteersStatus = {};
//   var processor = null;
//   if (args.local) {
//     log("local execution");
//     processor = pull.asyncMap(require(args.module)["/pando/1.0.0"]);

//     var io = {
//       source: args.items,
//       sink: pull(
//         pull.map(function (x) {
//           return String(x) + "\n";
//         }),
//         toPull.sink(process.stdout, function (err) {
//           log("process.stdout:done(" + err + ")");
//           if (err) {
//             console.error(err.message);
//             console.error(err);
//             process.exit(1);
//           }
//           process.exit(0);
//         })
//       ),
//     };

//     if (args["sync-stdio"]) {
//       log("synchronizing stdio");
//       io = sync(io);
//     }

//     log("executing function locally");
//     pull(
//       io,
//       pull.through(log),
//       probe("pando:input"),
//       processor,
//       probe("pando:result"),
//       pull.through(log),
//       io
//     );
//   } else {
//     var server = null;
//     var host = null;
//     // create local server or connect public server
//     if (!args.host) {
//       log("creating bootstrap server");
//       var publicDir = path.join(__dirname, "../local-server/public");
//       mkdirp.sync(publicDir);
//       server = new Server({
//         secret: args.secret,
//         publicDir: publicDir,
//         port: args.port,
//         seed: args.seed,
//       });
//       host = "localhost:" + args.port;

//       server._bootstrap.upgrade("/volunteer", function (ws) {
//         if (processor) {
//           log("volunteer connected over WebSocket");

//           ws.isAlive = true;
//           var heartbeat = setInterval(function ping() {
//             if (ws.isAlive === false) {
//               logHeartbeat("ws: volunteer connection lost");
//               return ws.terminate();
//             }
//             ws.isAlive = false;
//             ws.ping(function () {});
//           }, args.heartbeat);
//           ws.addEventListener("close", function () {
//             clearInterval(heartbeat);
//             heartbeat = null;
//           });
//           ws.addEventListener("error", function () {
//             clearInterval(heartbeat);
//             heartbeat = null;
//           });
//           ws.addEventListener("pong", function () {
//             logHeartbeat("ws: volunteer connection pong");
//             ws.isAlive = true;
//           });

//           processor.lendStream(function (err, stream) {
//             if (err) return log("error lender sub-stream to volunteer: " + err);
//             log("lending sub-stream to volunteer");

//             pull(
//               stream,
//               probe("volunteer-input"),
//               limit(duplexWs(ws), args["batch-size"]),
//               probe("volunteer-output"),
//               stream
//             );
//           });
//         }
//       });

//       server._bootstrap.upgrade("/volunteer-monitoring", function (ws) {
//         log("volunteer monitoring connected over WebSocket");

//         ws.isAlive = true;
//         var heartbeat = setInterval(function ping() {
//           if (ws.isAlive === false) {
//             logHeartbeat("ws: volunteer monitoring connection lost");
//             return ws.terminate();
//           }
//           ws.isAlive = false;
//           ws.ping(function () {});
//         }, args.heartbeat);
//         ws.addEventListener("close", function () {
//           clearInterval(heartbeat);
//           heartbeat = null;
//         });
//         ws.addEventListener("error", function () {
//           clearInterval(heartbeat);
//           heartbeat = null;
//         });
//         ws.addEventListener("pong", function () {
//           logHeartbeat("ws: volunteer monitoring pong");
//           ws.isAlive = true;
//         });

//         var id = null;
//         var lastReportTime = new Date();
//         pull(
//           duplexWs.source(ws),
//           pull.drain(
//             function (data) {
//               var info = JSON.parse(data);
//               id = info.id;
//               var time = new Date();
//               wsVolunteersStatus[info.id] = {
//                 id: info.id,
//                 timestamp: time,
//                 lastReportInterval: time - lastReportTime,
//                 performance: info,
//               };
//               lastReportTime = time;
//             },
//             function () {
//               if (id) {
//                 delete wsVolunteersStatus[id];
//               }
//             }
//           )
//         );
//       });

//       getIPAddresses().forEach(function (addr) {
//         console.error(
//           "Serving volunteer code at http://" + addr + ":" + args.port
//         );
//       });
//     } else {
//       log("using an external public bootstrap server");
//       host = args.host;
//       console.error("Serving volunteer code at http://" + host);
//     }

//     log("Serializing configuration for workers");
//     fs.writeFileSync(
//       path.join(__dirname, "../public/config.js"),
//       "window.pando = { config: " +
//         JSON.stringify({
//           batchSize: args["batch-size"],
//           degree: args.degree,
//           globalMonitoring: args["global-monitoring"],
//           iceServers: args["ice-servers"],
//           reportingInterval: args["reporting-interval"] * 1000,
//           requestTimeoutInMs: args["bootstrap-timeout"] * 1000,
//           version: package.version,
//         }) +
//         " }"
//     );

//     log("Uploading files to " + host + " with secret " + args.secret);
//     website.upload(
//       [
//         bundlePath,
//         path.join(__dirname, "../public/config.js"),
//         path.join(__dirname, "../public/index.html"),
//         path.join(__dirname, "../public/volunteer.js"),
//         path.join(__dirname, "../public/simplewebsocket.min.js"),
//         path.join(
//           __dirname,
//           "../node_modules/bootstrap/dist/css/bootstrap.min.css"
//         ),
//         path.join(
//           __dirname,
//           "../node_modules/bootstrap/dist/js/bootstrap.min.js"
//         ),
//         path.join(__dirname, "../node_modules/jquery/jquery.min.js"),
//         path.join(
//           __dirname,
//           "../node_modules/popper.js/dist/umd/popper.min.js"
//         ),
//       ],
//       host,
//       args.secret,
//       function (err) {
//         if (err) throw err;
//         log("files uploaded successfully");

//         log("connecting to bootstrap server");
//         var bootstrap = new BootstrapClient(host);

//         log("creating root node");
//         var root = new Node(bootstrap, {
//           requestTimeoutInMs: args["bootstrap-timeout"] * 1000, // ms
//           peerOpts: { wrtc: wrtc, config: { iceServers: args["ice-servers"] } },
//           maxDegree: args.degree,
//         }).becomeRoot(args.secret);

//         processor = createProcessor(root, {
//           batchSize: args["batch-size"],
//           bundle: !args["start-idle"]
//             ? require(bundlePath)["/pando/1.0.0"]
//             : function (x, cb) {
//                 console.error(
//                   "Internal error, bundle should not have been executed"
//                 );
//               },
//           globalMonitoring: args["global-monitoring"],
//           reportingInterval: args["reporting-interval"] * 1000, // ms
//           startProcessing: !args["start-idle"],
//         });

//         processor.on("status", function (rootStatus) {
//           var volunteers = {};

//           // Adding volunteers connected over WebSockets
//           for (var id in wsVolunteersStatus) {
//             volunteers[id] = wsVolunteersStatus[id];
//           }

//           // Adding volunteers connected over WebRTC
//           for (var id in rootStatus.children) {
//             volunteers[id] = rootStatus.children[id];
//           }

//           var status = JSON.stringify({
//             root: rootStatus,
//             volunteers: volunteers,
//             timestamp: new Date(),
//           });

//           logMonitoring(status);
//           logMonitoringChildren(
//             "children nb: " +
//               rootStatus.childrenNb +
//               " leaf nb: " +
//               rootStatus.nbLeafNodes
//           );

//           if (statusSocket) {
//             log("sending status to monitoring page");
//             statusSocket.send(status);
//           }
//         });

//         function close() {
//           log("closing");
//           if (server) server.close();
//           if (root) root.close();
//           if (bootstrap) bootstrap.close();
//           if (wrtc) wrtc.close();
//           if (processor) processor.close();
//         }

//         var io = {
//           source: args.items,
//           sink: pull.drain(
//             function (x) {
//               process.stdout.write(String(x) + "\n");
//             },
//             function (err) {
//               log("drain:done(" + err + ")");
//               if (err) {
//                 console.error(err.message);
//                 console.error(err);
//                 close();
//                 process.exit(1);
//               } else {
//                 close();
//                 process.exit(0);
//               }
//             }
//           ),
//         };

//         if (args["sync-stdio"]) {
//           io = sync(io);
//         }

//         pull(
//           io,
//           pull.through(log),
//           probe("pando:input"),
//           processor,
//           probe("pando:result"),
//           pull.through(log),
//           io
//         );
//       }
//     );

//     log("Starting monitoring server");
//     var app = express();
//     app.use(express.static(path.join(__dirname, "../root")));
//     var monitoringPort = args.port + 1;
//     var wss = WebSocket.Server({
//       server: http.createServer(app).listen(monitoringPort),
//     });
//     wss.on("connection/root-status", function (socket) {
//       statusSocket = socket;
//       socket.onerror = function () {
//         statusSocket = null;
//       };
//       socket.onclose = function () {
//         statusSocket = null;
//       };
//     });
//     getIPAddresses().forEach(function (addr) {
//       console.error(
//         "Serving monitoring page at http://" + addr + ":" + monitoringPort
//       );
//     });
//   }
// });
