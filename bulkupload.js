
var bulkuploader = function (client) {
  var BulkUploader = this
  BulkUploader.client = client
  BulkUploader.queue = []
  BulkUploader.haltflag = false
}

bulkuploader.prototype.addUpload = function (object) {
  var BulkUploader = this
  if (BulkUploader.haltflag) {
    throw new Error('Tried to add upload when uploader is shutting down')
  }
  BulkUploader.queue.push(object)
}

bulkuploader.prototype.runUpload = function () {
  var BulkUploader = this
  if (BulkUploader.queue.length >= 50) {
    var batch = BulkUploader.queue.splice(0, 50)
    BulkUploader._upload(batch)
  } else if (BulkUploader.haltflag) {
    if (BulkUploader.queue.length === 0) {
      return
    } else {
      BulkUploader._upload(BulkUploader.queue)
      BulkUploader.queue = []
      return
    }
  } else {
    setTimeout(BulkUploader.runUpload.bind(BulkUploader), 500)
  }
}

bulkuploader.prototype._upload = function (documentArray) {
  var BulkUploader = this
  var commandArray = []
  for (var i = 0; i < documentArray.length; i++) {
    commandArray.push(
      {index: {
        _index: documentArray[i].index,
        _type: documentArray[i].type
      }
      })
    commandArray.push(documentArray[i].body)
  }
  BulkUploader.client.bulk({body: commandArray}, function (err) {
    if (err) throw err
    BulkUploader.runUpload()
  })
}

bulkuploader.prototype.shutdown = function () {
  var BulkUploader = this
  BulkUploader.haltflag = true
}

module.exports = bulkuploader
