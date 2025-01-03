const { MongoClient } = require('mongodb');

const url = process.env.MONGO_URI;
const dbName = process.env.DB_NAME;

let db;
const connectDB = async () => {
    if (db) return db; // Return the existing db instance if already connected
    try {
        const client = await MongoClient.connect(url);
        db = client.db(dbName);
        console.log('Connected to MongoDB');
        return db;
    } catch (err) {
        console.error('MongoDB connection error:', err);
        throw err;
    }
};

module.exports = connectDB;
