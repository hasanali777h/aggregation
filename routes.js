'use strict';
const router = require("express").Router();
const aggragation = require("./aggregation");
// router.get("/get-all", aggragation.replaceRoot);
router.get("/get-all", aggragation.getAll);
module.exports = router;
