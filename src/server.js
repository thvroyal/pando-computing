require("dotenv").config();
// var cors = require('cors')
var portfinder = require("portfinder");
const { Project } = require("../bin/index");
const grpc = require("grpc");
const protoLoader = require("@grpc/proto-loader");
const path = require("path");
const { getPublicAddress, getInput } = require("./helpers");

const PROTO_PATH = path.join(__dirname, "/distributor.proto");

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

let client = null;
let isConnected = false;

const clientProto =
  grpc.loadPackageDefinition(packageDefinition).compute_engine;
const metadata = new grpc.Metadata().add("worker", "pando-1");

const run = (projectID, input, callback) => {
  portfinder.getPort(function (err, port) {
    if (err) throw err;

    console.log(`Running with input: ${input}`);
    const project = new Project({
      port,
      // module: "examples/square-no-delay.js",
      items: input,
      projectID: projectID,
    });

    project.start();
    // Pass the port value to the callback function
    callback(port);
  });
};

function createClient() {
  client = new clientProto.MyService(
    process.env.DISTRIBUTOR_HOST,
    grpc.credentials.createInsecure()
  );

  const deadline = new Date();
  deadline.setSeconds(deadline.getSeconds() + 5);

  client.waitForReady(deadline, (error) => {
    if (error) {
      console.log(error);
      scheduleReconnect();
      return;
    }
    const call = client.runProject(metadata);

    call.on("error", (error) => {
      console.error(
        "Connection to gRPC server closed! Trying to connect again..."
      );
      isConnected = false;
      scheduleReconnect();
    });

    call.on("end", () => {
      console.log(
        "Connection to gRPC server closed! Trying to connect again..."
      );
      isConnected = false;
      scheduleReconnect();
    });

    isConnected = true;
    console.log("Connected to gRPC server");

    call.on("data", (project) => {
      const { id } = project;
      console.log(`New project will be created: ${id}`);

      getInput(id)
        .then((inputList) => {
          input = inputList["input.txt"];
          Project.prototype.addOutput = function (
            bucketId,
            value,
            userId,
            totalOutput
          ) {
            try {
              client.addOutput(
                {
                  value,
                  createdAt: new Date().toISOString(),
                  bucketId,
                  userId,
                  totalOutput,
                },
                (error) => {
                  if (error) {
                    console.error(error);
                  }
                }
              );
            } catch (error) {
              console.error(error.message);
            }
          };
          Project.prototype.reportProjectStatus = function (data, bucketId) {
            try {
              client.report(
                {
                  data,
                  bucketId,
                },
                (error) => {
                  if (error) {
                    console.error(error);
                  }
                }
              );
            } catch (error) {
              console.error(error.message);
            }
          };
          run(id, input, async (port) => {
            const host = await getPublicAddress();
            call.write({
              status: 200,
              host,
              port,
              msg: "Created project successfully",
            });
          });
        })
        .catch((error) => {
          console.log("Error:", error);
        });
    });
  });
}

function scheduleReconnect() {
  if (isConnected) {
    return;
  }
  // Retry connection after a delay (e.g., 5 seconds)
  setTimeout(createClient, 5000);
}

createClient();
