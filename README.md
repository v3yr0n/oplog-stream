# oplog-stream
MongoDB oplog stream wrapper

## Installation
```
  npm install veyron-oplog-stream
```

## Usage
```js
  var oplog = new OplogStream('mongodb://192.168.1.1/local');
  oplog.firstSync((stream) => {
    stream.on('data', function (data) {
      console.log(data);
    });
    stream.on('error', function (err) {
      throw err;
    });
  });
```

## Destroying stream

```js
  stream.destroy() // also closes connection to db
```
