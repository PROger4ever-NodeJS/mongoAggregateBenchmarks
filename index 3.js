const mongo = require("mongodb");

const TEST_REPEATS = 1000;

let date = new Date("2019-05-02T11:00:03.0Z");

function geoNearWithMatch(collection, i, mainCB) {
  if (++i < TEST_REPEATS) {
    collection.aggregate([{
        '$geoNear': {
          near: {
            type: 'Point', coordinates: [33.4274736, 59.815999]
          },
          maxDistance: 50000,
          spherical: true,
          distanceField: 'dst',
          query: {
            create_date: { $gte: date }
          }
        }
      }, {
        '$project': {
          _id: 1,
        }
      }],
      function (err, doc) {
        if (err) {
          throw err; //mainCB(err);
        }
        geoNearWithMatch(collection, i, mainCB);
      });
  } else {
    mainCB();
  }
}

function geoNearWithQuery(collection, i, mainCB) {
  if (++i < TEST_REPEATS) {
    collection.aggregate([{
        $match: {
          create_date: { $gte: date }
        }
      }, {
        '$project': {
          _id: 1,
        }
      }],
      function (err, doc) {
        if (err) {
          throw err; //mainCB(err);
        }
        geoNearWithQuery(collection, i, mainCB);
      });
  } else {
    mainCB();
  }
}

function start(mainCB) {
  mongo.connect("mongodb://mongo:27017/geo", function (err, db) {
    let collection = db.collection("cities");
    console.log("start mongo...");
    let timeStart = new Date();
    geoNearWithMatch(collection, -1, function (err, res) {
      let diff = new Date() - timeStart;
      console.log("geoNearWithMatch... ", diff / 1000);

      timeStart = new Date();
      geoNearWithQuery(collection, -1, function (err, res) {
        let diff = new Date() - timeStart;
        console.log("geoNearWithQuery... ", diff / 1000);
        db.close(mainCB.bind(null));
      });
    });
  });
}


start(() => {
  console.log("end");
});