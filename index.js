require('dotenv/config');
const express = require('express');
const app = express();
const router = require('./routes');
const port = process.env.PORT || 3000;
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use('/',router);
(()=>{
    try {
        app.listen(port,()=>{
            console.log(`Server running on port ${port}`);
        });
    } catch (error) {
        console.log(error);
    }
})();