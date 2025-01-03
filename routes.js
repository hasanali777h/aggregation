const router = require('express').Router();
const aggragation = require('./aggregation');
router.get('/get-all', aggragation.filter);
module.exports = router;