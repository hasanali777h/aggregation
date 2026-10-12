"use strict";
const mongoose = require("mongoose");

const connectDB = async (url) => {
  if (mongoose.connection.readyState === 1) {
    return mongoose.connection; // already connected
  }
  await mongoose.connect(url);
  console.log("mongodb connected...");
  return mongoose.connection;
};

// Getter for the native db instance (use after connectDB has run)
const getDB = () => {
  if (mongoose.connection.readyState !== 1) {
    throw new Error("Database not connected yet");
  }
  return mongoose.connection.db;
};

module.exports = { connectDB, getDB };
