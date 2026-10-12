"use strict";
const dns = require("node:dns");
dns.setServers(["8.8.8.8", "1.1.1.1"]);
const path = require("path");
const dotenv = require("dotenv");
dotenv.config({ path: path.resolve(__dirname, ".env") });
const express = require("express");
const app = express();
const router = require("./routes");
const { connectDB } = require("./db");

const port = process.env.PORT ?? 3000;
const url = process.env.MONGO_URI;

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use("/", router);

const startServer = async () => {
  app.listen(port, () => {
    console.log(`Server running on port ${port}`);
  });
  await connectDB(url);
};
startServer();
