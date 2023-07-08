require('dotenv').config()
// var cors = require('cors')
var portfinder = require("portfinder");
const { Project } = require("../bin/index");
const AWS = require("aws-sdk");
var allSettled = require("promise.allsettled");
const grpc = require('grpc');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const http = require('http')

const PROTO_PATH = path.join(__dirname, '/distributor.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

function getPublicAddress() {
  return new Promise((resolve, reject) => {
    http.get('http://checkip.amazonaws.com', (res) => {
      let publicAddress = '';
      res.on('data', (chunk) => {
        publicAddress += chunk;
      });
      
      res.on('end', () => {
        resolve(publicAddress)
      })
      
      res.on('error', (err) => {
        reject(err)
      })
    })
  })
}

const clientProto = grpc.loadPackageDefinition(packageDefinition).io.mark.grpc.grpcChat;

const gRPClient = new clientProto.MyService(process.env.DISTRIBUTOR_HOST, grpc.credentials.createInsecure());

const metadata = new grpc.Metadata();
metadata.add('worker', 'pando-1');
const distributorCall = gRPClient.RunProject(metadata);

// gRPClient.Ping({}, metadata, (error, response) => {
//   if (error || response.status !== 200) {
//     throw new Error('Failed to connect with distributor-service. Please try again!');
//   }
//   console.log(response.msg);
// });

function getInput(projectID) {
  const s3 = new AWS.S3();
  const bucketName = "mybucketforpando";

  const params = {
    Bucket: bucketName,
    Prefix: projectID,
  };

  return new Promise((resolve, reject) => {
    const listInput = {};

    s3.listObjects(params, (err, data) => {
      if (err) {
        reject(err);
      } else {
        const getObjectPromises = data.Contents.map((object) => {
          const getObjectParams = {
            Bucket: bucketName,
            Key: object.Key,
          };

          if (object.Key.includes("input")) {
            return new Promise((resolve, reject) => {
              s3.getObject(getObjectParams, (getObjectErr, getObjectData) => {
                if (getObjectErr) {
                  console.error("Error retrieving object:", getObjectErr);
                  reject(getObjectErr);
                } else {
                  const parts = object.Key.split("/"); // Split the string into an array
                  const key = parts[1];
                  listInput[key] = getObjectData.Body.toString().split("\n");
                  resolve();
                }
              });
            });
          } else {
            return Promise.resolve();
          }
        });

        allSettled(getObjectPromises)
          .then(() => {
            resolve(listInput);
          })
          .catch((error) => {
            reject(error);
          });
      }
    });
  });
}

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

distributorCall.on('data', (project) => {
  console.log(project);
  const { id } = project;
  
  getInput(id).then((inputList) => {
    input = inputList["input.txt"];
    
    // Call the run function and pass a callback function
    run(id, input, async (port) => {
      // Send back the port as the response
      const host = await getPublicAddress();
      distributorCall.write({ status: 200, host, port, msg: 'Created project successfully'})
    });
  })
  .catch((error) => {
    // Handle any errors that occur during the getInput operation
    console.log("Error:", error);
    res
      .status(500)
      .send({ error: "An error occurred while fetching input data." });
  })
})

distributorCall.on('end', () => {
  // Server has ended the stream
  console.log('Server closed the stream');
});
