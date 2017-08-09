'use strict';
const MongoDB = require('mongodb')
var mongoUrl = "";

module.exports = class OplogStream {
    
    constructor(mongoUrl = "mongodb://localhost/local") {
      this.mongoUrl = mongoUrl
    }

    /** Creates the connection to MongoDB */
    _connection(callback){
        MongoDB.MongoClient.connect(this.mongoUrl, function (err, db) {
            if (err) {
              console.log(err)
              throw err
            }
            db.collection('oplog.rs', function (err, oplog) {
                if (err) {
                  console.log(err)
                  throw err
                } else {
                  callback(oplog)
                }
            })
        });
    }
    
    /** Gets all documents from the oplog based on the optional filter 
     * @param {function} callback to be called after quering the oplog
     * @param {object} filter to used in the oplog query
    */
    firstSync(callback, filter = {}) {
      this._connection((oplog)=>{
        var timestamp = MongoDB.Timestamp(0, 0);
        var allFilters = Object.assign(filter, {ts: {$gt: timestamp}});
        var stream = this._createStream(oplog, allFilters, false);
        callback(stream)
      });
    }

    /** Gets all documents from the oplog based on the optional filter and since some date
     * @param {number} since something like Date.now() (timestamp)
     * @param {function} callback to be called after quering the oplog
     * @param {object} filter to used in the oplog query
     */
    syncSince(since, callback, filter = {}){
      this._connection((oplog)=>{
        //var timestamp = MongoDB.Timestamp(0, (since / 1000 | 0))
        var timestamp = MongoDB.Timestamp.fromString(since)
        var allFilters = Object.assign(filter, {ts: {$gt: timestamp}});
        let stream = this._createStream(oplog, allFilters, false);
        callback(stream)
      });
    }

    /** Gets all documents from the oplog based on the optional filter 
     *  and awaits new records to be added to MongoDB 
     * @param {function} callback to be called after quering the oplog
     * @param {object} filter to used in the oplog query
    */
    firstSyncAndStream(callback, filter = {}) {
      this._connection((oplog)=>{
        var timestamp = MongoDB.Timestamp(0, 0);
        var allFilters = Object.assign({ts: {$gt: timestamp}}, filter);
        var stream = this._createStream(oplog, allFilters);
        callback(stream)
      });
    }

    /** Gets all documents from the oplog based on the optional filter, since some date
     *  and awaits new records to be added to MongoDB 
     * @param {number} since something like Date.now() (timestamp)
     * @param {function} callback to be called after quering the oplog
     * @param {object} filter to used in the oplog query
     */
    syncSinceAndStream(since, callback, filter = {}){
      this._connection((oplog)=>{
        //var timestamp = MongoDB.Timestamp(0, (since / 1000 | 0))\
        var timestamp = MongoDB.Timestamp.fromString(since)
        console.log("syncing since:", timestamp);
        var allFilters = Object.assign(filter, {ts: {$gt: timestamp}});
        var self = this;
        var count = oplog.count(allFilters, function(err, count){
          let stream = self._createStream(oplog, allFilters);
          callback(stream, count)
        });
      });
    }

    /** Starts streaming documents since the current date.
     * @param {function} callback to be called after quering the oplog
     * @param {object} filter to used in the oplog query
     */
    startStream(callback, filter = {}){
      this._connection((oplog)=>{
        var now = Date.now()
        var timestamp = MongoDB.Timestamp(0, (now / 1000 | 0))
        var allFilters = Object.assign({ts: {$gt: timestamp}}, filter);
        let stream = this._createStream(oplog, allFilters);

        callback(stream)
      });
    }

    saveOplog(callback, data){
      this._connection((oplog)=>{
        console.log('trying to save');
        oplog.save(data);
        console.log('saved?');
        //callback(stream)
      });
    }

    /**
     * Private function to create and return the stream object.
     * If stream = true the cursor will be awaiting updates/deletes 
     * or inserts of documents
     * @param {db.oplog.rs} oplog 
     * @param {object} filter 
     * @param {boolean} stream 
     */
    _createStream(oplog, filter, tailable=true){
      var cursor = oplog.find(filter, {
        tailable: tailable,
        awaitdata: tailable,
        oplogReplay: true,
        numberOfRetries: -1
      })
      var stream = cursor.stream()
      var destroy = stream.destroy
      stream.destroy = function () {
        console.log('stream destroy called');
        //cursor.close()
        //destroy.call(stream)
        //db.close()
      }

      return stream;
    }
}