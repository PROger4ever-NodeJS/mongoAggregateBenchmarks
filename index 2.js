const mongo = require("mongodb");

const TEST_REPEATS = 1000;

function geoNearWithMatch(collection, i, mainCB) {
  if (++i < TEST_REPEATS) {
    collection.aggregate([{
        '$geoNear': {
          near: { type: 'Point', coordinates: [37.6173, 55.755826] },
          maxDistance: 50000,
          spherical: true,
          distanceField: 'dst'
        }
      }, { '$match': { tags: { '$in': ['jdfjdjdj'] } } }, {
        '$project': {
          ad_id: 1,
          dst: 1,
          _id: 0
        }
      }, { '$sort': { 'grade.average': -1 } }],
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
        '$geoNear': {
          near: { type: 'Point', coordinates: [37.6173, 55.755826] },
          maxDistance: 50000,
          spherical: true,
          distanceField: 'dst',
          query: { tags: { '$in': ['jdfjdjdj'] } }
        }
      }, {
        '$project': {
          ad_id: 1,
          dst: 1,
          _id: 0
        }
      }, { '$sort': { 'grade.average': -1 } }],
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
  mongo.connect("mongodb://mongo:27017/otr", function (err, db) {
    let collection = db.collection("ads_view");
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