const connectDB = require("./db");

const getAll = async (req, res) => {
  try {
    const db = await connectDB(); // Wait for the database connection
    const data = await db.collection("users").find().limit(1).toArray();
    res.status(200).json(data);
  } catch (error) {
    res.status(500).json({ message: error.message });
    console.error(error);
  }
};

const lookup = async (req, res) => {
  try {
    const db = await connectDB(); // Wait for the database connection
    const pipeline = [
      {
        $lookup: {
          from: "comments",
          localField: "_id",
          foreignField: "movie_id",
          as: "show_comments",
        },
      },
      {
        $project: {
          title: 1,
          show_comments: {
            $slice: ["$show_comments", 5],
          },
        },
      },
    ];
    const data = await db.collection("movies").aggregate(pipeline).limit(10).toArray();
    const found = data.filter((element) => element.show_comments.length > 0);
    res.status(200).json(found);
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: error.message });
  }
};

const match = async (req, res) => {
  try {
    const db = await connectDB();
    const pipeline = [
      {
        $match: {
          name: "Mercury",
        },
      },
      {
        $group: {
          _id: "$name",
          mean: { $avg: "$surfaceTemperatureC.mean" },
        },
      },
    ];
    const data = await db.collection("planets").aggregate(pipeline).toArray();
    res.status(200).json(data);
  } catch (error) {
    console.log(error);
  }
};

const unwind = async (req, res) => {
  try {
    const db = await connectDB();
    const pipeline = [
      {
        $unwind: {
          //works on arrays
          path: "$mainAtmosphere",
          preserveNullAndEmptyArrays: true,
        },
      },
    ];
    const data = await db.collection("planets").aggregate(pipeline).toArray();
    res.status(200).json(data);
  } catch (error) {
    console.log(error);
  }
};

const and = async (req, res) => {
  try {
    const db = await connectDB();
    const data = await db
      .collection("planets")
      .find({
        $and: [{ name: "Uranus" }, { "surfaceTemperatureC.mean": { $gt: 0 } }],
      })
      .toArray();
    res.status(200).json(data);
  } catch (error) {
    console.log(error);
  }
};

const or = async (req, res) => {
  try {
    const db = await connectDB();
    const data = await db
      .collection("planets")
      .find({
        $or: [{ name: "Uranus" }, { "surfaceTemperatureC.mean": { $gt: 0 } }],
      })
      .toArray();
    res.status(200).json(data);
  } catch (error) {
    console.log(error);
  }
};

const exp = async (req, res) => {
  try {
    const db = await connectDB();
    const pipeline = [
      {
        $match: {
          $expr: {
            //expression operator sould use in other operators to do some work
            $gt: ["$surfaceTemperatureC.mean", 50],
          },
        },
      },
    ];
    const data = await db.collection("planets").aggregate(pipeline).toArray();
    res.status(200).json(data);
  } catch (error) {
    console.log(error);
  }
};

const $in = async (req, res) => {
  try {
    const db = await connectDB();
    const pipeline = [
      {
        $match: {
          mainAtmosphere: {
            $in: ["H", "He"],
          },
        },
      },
    ];
    // const data = await db.collection('planets').find(...pipeline).toArray();
    const data = await db.collection("planets").aggregate(pipeline).toArray();
    res.status(200).json(data);
  } catch (error) {
    console.log(error);
  }
};

const ne = async (req, res) => {
  try {
    const db = await connectDB();
    const pipeline = [
      {
        $project: {
          _id: 0,
          name: 1,
          orderFromSun: 1,
          hasRings: 1,
          remains: {
            $ne: ["$name", "Earth"],
          }, //$ne should use in other stage operators to do some work and takes array[fieldName,valueToMatch] and return boolean value
        },
      },
    ];
    const data = await db.collection("planets").aggregate(pipeline).toArray();
    res.status(200).json(data);
  } catch (error) {
    console.log(error);
  }
};

const not = async (req, res) => {
  try {
    const db = await connectDB();
    const pipeline = [
      {
        $project: {
          eliminated: {
            $not: [
              {
                $gt: ["$surfaceTemperatureC.mean", 50],
              },
            ],
          },
        },
      },
    ];
    const data = await db.collection("planets").aggregate(pipeline).toArray();
    res.status(200).json(data);
  } catch (error) {
    console.log(error);
  }
};

const push = async (req, res) => {
  try {
    const db = await connectDB();
    const pipeline = [
      {
        $sort: {
          orderFromSun: 1,
        },
      },
      {
        $group: {
          _id: "$hasRings",
          planets: {
            $push: "$name", // to use push first we need to sort and then group after that we can use push
          },
        },
      },
    ];
    const data = await db.collection("planets").aggregate(pipeline).toArray();
    res.status(200).json(data);
  } catch (error) {
    console.log(error);
  }
};

const map = async (req, res) => {
  try {
    const db = await connectDB();
    const pipeline = [
      {
        $project: {
          increasedOrderFromSun: {
            $map: {
              //map is used to iterate over an array and do some work on each element
              input: "$mainAtmosphere",
              as: "name",
              in: {
                $add: ["$orderFromSun", 1],
              },
            },
          },
        },
      },
    ];
    const data = await db.collection("planets").aggregate(pipeline).toArray();
    res.status(200).json(data);
  } catch (error) {
    console.log(error);
  }
};

const count = async (req, res) => {
  try {
    const db = await connectDB();
    const pipeline = [
      {
        $group: {
          _id: "$name",
        },
      },
      {
        $count: "total",
      },
    ];
    const data = await db.collection("planets").aggregate(pipeline).toArray();
    res.status(200).json(data);
  } catch (error) {
    console.log(error);
  }
};

const addField = async (req, res) => {
  try {
    const db = await connectDB();
    const pipeline = [
      {
        $addFields: {
          meanAndOrder: {
            $concat: ["$name", "-", "$name"],
          },
        },
      },
      {
        $addFields: {
          decimalOrder: {
            $toDecimal: "$orderFromSun",
          },
        },
      },
      {
        $addFields: {
          increasedDecimalOrder: {
            $add: ["$decimalOrder", 1],
          },
        },
      },
      {
        $addFields: {
          total: {
            $add: ["$orderFromSun", 1],
          },
        },
      },
    ];
    const data = await db.collection("planets").aggregate(pipeline).toArray();
    res.status(200).json(data);
  } catch (error) {
    console.log(error);
  }
};

const cond = async (req, res) => {
  try {
    const db = await connectDB();
    const pipeline = [
      {
        $project: {
          data: {
            $cond: {
              if: {
                $lte: [{ $size: "$mainAtmosphere" }, 0],
              },
              then: "No Atmosphere",
              else: "Has Atmosphere",
            },
          },
        },
      },
    ];
    const data = await db.collection("planets").aggregate(pipeline).toArray();
    res.status(200).json(data);
  } catch (error) {
    console.log(error);
  }
};

const filter = async (req, res) => {
  try {
    const db = await connectDB();
    const pipeline = [
      {
        $project: {
          mainAtmosphere: {
              $filter: {
                 input: "$mainAtmosphere",
                 as: "data",
                 cond: { $gt: [ "$$data", 1 ] },
                },
              },
        }
     }
      // {
      //   $project: {
      //     orderFromSun: {
      //       $filter: {
      //         input: "$orderFromSun",
      //         as: "order",
      //         $cond: {
      //           if: { $lt: ["$$order", 5] },
      //           then: "$$order",
      //           else: null,
      //         },
      //       },
      //     },
      //   },
      // },
    ];
    const data = await db.collection("planets").aggregate(pipeline).toArray();
    res.status(200).json(data);
  } catch (error) {
    console.log(error);
  }
};

// const func = async(req,res)=>{
//   try {
//     const db = await connectDB();
//     const pipeline = [];
//     const data = await db.collection('planets').aggregate(pipeline).toArray();
//     res.status(200).json(data);
//   } catch (error) {
//     console.log(error)
//   }
// }

module.exports = {
  getAll,
  lookup,
  match,
  unwind,
  and,
  or,
  exp,
  $in,
  ne,
  not,
  push,
  map,
  count,
  addField,
  cond,
  filter,
};
