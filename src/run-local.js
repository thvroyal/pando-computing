require('dotenv').config()
const { getInput } = require("./helpers");
var portfinder = require("portfinder");
const { Project } = require("../bin/index");

const id = 'd5d3f590-9eaa-40b9-bb83-07b4e0f1b341';

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

getInput(id).then((inputList) => {
  input = inputList["input.txt"];
  run(id, input, async (port) => {
    console.log('Running...')
  });
})
.catch((error) => {
  console.log("Error:", error);
})