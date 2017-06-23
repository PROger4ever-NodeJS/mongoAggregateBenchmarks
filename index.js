const mongo = require("mongodb");

const TEST_REPEATS = 1000;

let date = new Date("2017-05-02T11:00:03.0Z");

function aggregate(collection, i) {
  if (++i < TEST_REPEATS) {
    let x = collection.aggregate([{
      $match: {
        create_date: date
      }
    }, {
      '$project': {
        _id: 1,
      }
    }]);
    return x.toArray().then(function (doc) {
      return aggregate(collection, i);
    });
  }
}

function find(collection, i, mainCB) {
  if (++i < TEST_REPEATS) {
    let x = collection.find({
      create_date: date
    }, {
      fields: {
        _id: 1
      }
    });
    return x.toArray().then(function (doc) {
      return find(collection, i);
    });
  }
}

function start(mainCB) {
  mongo.connect("mongodb://mongo:27017/geo").then(function (db) {
    let collection = db.collection("cities");
    console.log("start mongo...");
    let timeStart = new Date();
    aggregate(collection, -1).then(function (res) {
      let diff = new Date() - timeStart;
      console.log("aggregate... ", diff / 1000);

      timeStart = new Date();
      find(collection, -1).then(function (res) {

        let diff = new Date() - timeStart;
        console.log("find... ", diff / 1000);
        db.close(mainCB.bind(null));
      });
    });
  });
}


start(() => {
  console.log("end");
});