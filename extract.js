var fs = require('graceful-fs')
var index = require('./index.js')
var recursive = require('recursive-readdir')
var Entities = require('html-entities').XmlEntities
var entities = new Entities()
var _ = require('lodash')
var debug = require('debug')('canary-perch:extract')
var BulkUploader = require('./bulkupload')
var cprocess = require('child_process')

var numberOfFiles
var finished

var extractor = function (hosts, inputIndex, outputIndex, inputType, outputType, dictDir) {
  this.hosts = hosts
  this.inputIndex = inputIndex
  this.outputIndex = outputIndex
  this.inputType = inputType
  this.outputType = outputType
  this.dictDir = dictDir
  this.client = index.ESClient(hosts)
  this.uploader = new BulkUploader(this.client)
  this.uploader.runUpload()
}

extractor.prototype.setDictHash = function (folder) {
  var Extractor = this
  Extractor.dictHash = cprocess.execSync('git -C ' + folder + ' rev-parse HEAD', {encoding: 'utf8'}).trim()
}

extractor.prototype.readDictionaries = function (dictionaryWhitelist) {
  var Extractor = this
  var folder = Extractor.dictDir + '/json/'
  var client = Extractor.client
  Extractor.setDictHash(folder)
  console.log(Extractor.dictHash)
  debug('starting extractions with dictionaries from: ' + folder)
  recursive(folder, function (err, files) {
    if (err) throw err
    numberOfFiles = files.length
    finished = _.after(numberOfFiles, () => {
      if (err) throw err
      console.log('all extractions finished')
      Extractor.uploader.shutdown()
    })
    files.forEach(function (file) {
      fs.readFile(file, 'utf8', function (err, data) {
        if (err) throw err
        if (!(dictionaryWhitelist)) {
          Extractor.dictionaryQuery(JSON.parse(data), client)
        } else {
          if (dictionaryWhitelist.find(data.id)) {
            Extractor.dictionaryQuery(JSON.parse(data), client)
          } else {
            finished()
          }
        }
      })
    })
  })
}

// Pass it the full dictionary first time. On the last successful upload of data
// run again with the first entry removed and repeate until empty
extractor.prototype.dictionaryQuery = function (dictionary, client) {
  var Extractor = this
  if (!dictionary.entries) {
    debug('Malformed Dictionary: ' + dictionary)
    return
  }
  if (dictionary.entries.length) {
    var entry = dictionary.entries.shift()
    Extractor.dictionarySingleQuery(entry, dictionary, client)
  } else {
    debug('finished extraction')
    finished()
    return
  }
}

extractor.prototype.handleDictionarySingleQueryResponse = function (error, response, dictionary, entry, client) {
  var Extractor = this
  if (error) {
    console.log(error)
  }
  if (!error) {
    if (response.hits.hits.length === 0) {
      Extractor.dictionaryQuery.bind(Extractor, dictionary, client)()
    } else {
      debug('got back' + response.hits.hits.length + 'hits searching for term: ' + entry.term)
      for (var j = 0; j < response.hits.hits.length; j++) {
        Extractor.uploadOneDocFacts(response.hits.hits[j], dictionary, entry, client)
      }

      Extractor.dictionaryQuery.bind(Extractor, dictionary, client)()
    }
  }
}

extractor.prototype.dictionarySingleQuery = function (entry, dictionary, client) {
  var Extractor = this
  var callBacker = function (error, response) {
    Extractor.handleDictionarySingleQueryResponse.bind(Extractor, error, response, dictionary, entry, client)()
  }
  client.search({
    index: Extractor.inputIndex,
    body: {
      _source: false,
      fields: ['cprojectID'],
      query: {
        match_phrase: {
          fulltext: entry.term
        }
      },
      highlight: {
        encoder: 'html',
        fields: {
          fulltext: { boundary_chars: '.,!?\t\n' }
        }
      }
    }
  }, callBacker)
}

// insert all the facts from one document as returned by ES
extractor.prototype.uploadOneDocFacts = function (oneDocFacts, dictionary, entry, client) {
  var Extractor = this
  // console.log('snippet array is: ' + snippetArray)
  var snippetArray = oneDocFacts.highlight.fulltext
  for (var i = 0; i < snippetArray.length; i++) {
    var match = snippetArray[i]
    var fact = {}
    if (match.indexOf('<em>') !== -1) {
      fact.prefix = match.split('<em>')[0]
      fact.term = match.split('<em>')[1].split('</em>')[0]
      fact.postfix = match.split('</em>')[1]
    } else {
      fact.prefix = ''
      fact.term = match
      fact.postfix = ''
    }
    fact.prefix = entities.decode(fact.prefix)
    fact.term = entities.decode(fact.term)
    fact.postfix = entities.decode(fact.postfix)
    fact.docId = oneDocFacts._id
    fact.cprojectID = oneDocFacts.fields.cprojectID
    Extractor.uploadOneFact(fact, dictionary, entry, client)
  }
}

extractor.prototype.uploadOneFact = function (fact, dictionary, entry, client) {
  var Extractor = this
  // console.log("uploading one fact")
  var d = new Date()
  Extractor.uploader.addUpload({
    index: Extractor.outputIndex,
    type: Extractor.outputType,
    body: {
      'prefix': fact.prefix,
      'post': fact.postfix,
      'term': fact.term,
      'documentID': fact.docId,
      'cprojectID': fact.cprojectID,
      'identifiers': entry.identifiers,
      'ingestionDate': d.toJSON(),
      'dictionaryHash': Extractor.dictHash
    }
  })
}

module.exports = extractor
