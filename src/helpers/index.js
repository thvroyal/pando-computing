const http = require('http')
const AWS = require("aws-sdk");
var allSettled = require("promise.allsettled");

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

module.exports = {
  getPublicAddress,
  getInput,
}
