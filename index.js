var recursive = require('recursive-readdir')
var elasticsearch = require('elasticsearch')
var _ = require('lodash')
var path = require('path')
var fs = require('graceful-fs')
var AgentKeepAlive = require('agentkeepalive')
var Elasticdump = require('elasticdump')
var debug = require('debug')('canary-perch:index')

// =====================================================================================
// FUNCTIONS TO GET CONTENT INTO THE INDEX
// recursively looks through a given folder to upload eupmc_result.json contents
// to elastic search
var ESClient = function (hosts) {
  if (!hosts) throw new Error('no elastichost host')
  var client = new elasticsearch.Client({
    //log: 'trace',
    hosts: hosts,
    maxSockets: 20,
    maxRetries: 50,
    createNodeAgent: function (connection, config) {
      return new AgentKeepAlive(connection.makeAgentConfig(config))
    }
  })
  return client
}

var errorPrintingCB = function (error) {
  if (error) {
    console.log(error)
  }
}

var uploadJSONFileToES = function (file, index, type, client, cprojectID, cb) {
  fs.readFile(file, function (err, data) {
    if (err) throw err
    var document = JSON.parse(data)
    document.cprojectID = cprojectID
    client.create({
      index: index,
      type: type,
      body: document
    }, cb)
  })
}

var uploadXMLFileToES = function (file, index, type, client, cprojectID, cb) {
  fs.readFile(file, function (err, data) {
    if (err) throw err
    client.index({
      index: index,
      type: type,
      body: {
        'fulltext': data.toString('utf8'),
        'cprojectID': cprojectID
      }
    }, cb)
  })
}

var loadEuPMCFullTexts = function (folder, hosts, index, cb) {
  var client = ESClient(hosts)
  console.log('reading fulltexts from disk')
  recursive(folder, function (err, files) {
    if (err) throw err
    var errorWrappingDone = function (err) {
      if (err) throw err
      done()
    }
    var done = _.after(files.length, function () {
      cb()
      console.log('done all loading of files')
    })
    debug('list of files to consider: %O', files)
    files.forEach(function (file) {
      if (path.basename(file) === 'fulltext.xml') {
        var cprojectID = path.basename(path.dirname(file))
        debug('uploading fulltext from CProject: ' + cprojectID)
        uploadXMLFileToES(file, index, 'unstructured', client, cprojectID, errorWrappingDone)
      } else {
        done()
      }
    })
  })
}

var loadCRHTMLFullTexts = function (folder, hosts, cb) {
  loadCRFullTexts(folder, 'fulltext.html', hosts, cb)
}

var loadCRXHTMLFullTexts = function (folder, hosts, cb) {
  loadCRFullTexts(folder, 'fulltext.xhtml', hosts, cb)
}

var loadCRPDFFullTexts = function (folder, hosts, cb) {
  loadCRFullTexts(folder, 'fulltext.pdf.txt', hosts, cb)
}

var loadCRFullTexts = function (folder, filename, hosts, cb) {
  filename = filename || 'fulltext.html'
  var client = ESClient(hosts)
  console.log('reading fulltexts from disk')
  recursive(folder, function (err, files) {
    if (err) throw err
    var done = _.after(files.length, function () {
      cb()
      console.log('done all loading of files')
    })
    files.forEach(function (file) {
      if (path.basename(file) === filename) {
        var cprojectID = path.basename(path.dirname(file))
        // console.log("uploading fulltext from CProject: " + cprojectID)
        uploadXMLFileToES(file, 'fulltext', 'unstructured', client, cprojectID, done)
      } else {
        done()
      }
    })
  })
}

var indexEuPMCMetadata = function (folder, hosts, index) {
  var client = ESClient(hosts)
  console.log(folder)
  recursive(folder, function (err, files) {
    if (err) throw err
    files.forEach(function (file) {
      if (path.basename(file) === 'eupmc_result.json') {
        var cprojectID = path.basename(path.dirname(file))
        // console.log("Uploading file with cprojectID: " + cprojectID)
        uploadJSONFileToES(file, index, 'eupmc', client, cprojectID, errorPrintingCB)
      }
    })
  })
}

var indexCRMetadata = function (folder, hosts) {
  var client = ESClient(hosts)
  console.log(folder)
  recursive(folder, function (err, files) {
    if (err) throw err
    files.forEach(function (file) {
      if (path.basename(file) === 'crossref_result.json') {
        var cprojectID = path.basename(path.dirname(file))
        // console.log("Uploading file with cprojectID: " + cprojectID)
        uploadJSONFileToES(file, 'metadata', 'crossref', client, cprojectID, errorPrintingCB)
      }
    })
  })
}

var deleteFactIndex = function (err, hosts, index, cb) {
  if (err) throw err
  var client = ESClient(hosts)
  // Use dummy callback to wedge in hosts
  var dummyCallback = function (err) {
    if ((err) && !(err.status === 404)) throw err
    cb(undefined, hosts, cb)
  }
  client.indices.delete({
    index: index
  }, dummyCallback)
}

var mapFactIndex = function (err, hosts, index, cb) {
  if ((err) && !(err.status === 404)) {
    console.log(err)
    throw err
  }
  var client = ESClient(hosts)
  client.indices.create({
    body: {
      'mappings': {
        'snippet': {
          'properties': {
            'cprojectID': {'type': 'string'},
            'documentID': {'type': 'string'},
            'identifiers': {
              'properties': {
                'contentmine': {'type': 'string'},
                'opentrials': {'type': 'string'}
              }
            },
            'post': {'type': 'string'},
            'prefix': {'type': 'string'},
            'term': {'type': 'string'}
          }
        }
      }
    },
    index: index
  }, cb)
}

var deleteAndMapFactIndex = function (err, hosts, index, cb) {
  if (err) throw err
  var dummyCallback = function () {
    mapFactIndex(undefined, hosts, index, cb)
  }
  deleteFactIndex(undefined, hosts, index, dummyCallback)
}

var deleteAndMapMetadataIndex = function (err, hosts, cb) {
  if (err) throw err
  deleteMetadataIndex(undefined, hosts, mapMetadataIndex)
}

var deleteMetadataIndex = function (err, hosts, cb) {
  if (err) throw err
  var client = ESClient(hosts)
  // dummy callbackto wedge in hosts
  var dummyCallback = function (err) {
    if (err) throw err
    if (Meteor) {
      Meteor.bindEnvironment(function () { cb(err, hosts) })
    }
    cb(err, hosts)
  }
  client.indices.delete({
    index: 'metadata'
  }, dummyCallback)
}

var mapMetadataIndex = function (err, hosts, cb) {
  if ((err) && !(err.status === 404)) {
    console.log(err)
    throw err
  }
  var client = ESClient(hosts)
  // ToDo: sort out mapping
  var metadataMapping = require('metadataMap.json')
  client.indices.create({
    index: 'facts',
    body: {
      mappings: metadataMapping
    }
  }
, cb)
}

var deleteAndMapUnstructuredPaperIndex = function (err, hosts, index, cb) {
  debug('deleting papers from hosts:' + hosts + ' and index:' + index)
  if (err) throw err
  var boundMapUnstructuredPaperIndex = function (err) {
    mapUnstructuredPaperIndex(err, hosts, index, cb)
  }
  deleteUnstructuredPaperIndex(undefined, hosts, index, boundMapUnstructuredPaperIndex)
}

var deleteUnstructuredPaperIndex = function (err, hosts, index, cb) {
  if (err) throw err
  var client = ESClient(hosts)
  function callback (err) {
    if ((err) && !(err.status === 404)) throw err
    cb()
  }
  client.indices.delete({index: index}, callback)
}

var mapUnstructuredPaperIndex = function (err, hosts, index, cb) {
  if (err) throw err
  var client = ESClient(hosts)
  client.indices.create({
    index: index,
    body: {
      'mappings': {
        'unstructured': {
          'properties': {
            'cprojectID': {'type': 'string'},
            'fulltext': {
              'type': 'string', 'term_vector': 'with_positions_offsets_payloads'
            }
          }
        }
      }
    }
  }, cb)
}

var dump = function (hosts, directory) {
  var defaultEDOptions = {
    limit: 100,
    offset: 0,
    debug: false,
    type: 'data',
    delete: false,
    maxSockets: null,
    input: 'http://' + hosts[0],
    'input-index': '_all',
    output: directory + '/' + 'dump-' + new Date().toISOString() + '.json',
    'output-index': null,
    inputTransport: null,
    outputTransport: null,
    searchBody: null,
    sourceOnly: false,
    jsonLines: false,
    format: '',
    'ignore-errors': false,
    scrollTime: '10m',
    timeout: null,
    toLog: null,
    awsAccessKeyId: null,
    awsSecretAccessKey: null
  }

  var date = new Date()
  var outfile = directory + '/' + 'dump-' + date.toISOString() + '.json'
  var ed = new Elasticdump.Elasticdump('http://' + hosts[0], outfile, defaultEDOptions)
  ed.on('log', function (message) { console.log('log' + message) })
  ed.on('debug', function (message) { console.log('debug' + message) })
  ed.on('error', function (error) { console.log('error' + 'Error Emitted => ' + (error.message || JSON.stringify(error))) })
  ed.dump()
}

module.exports.loadEuPMCFullTexts = loadEuPMCFullTexts
module.exports.ESClient = ESClient
module.exports.dump = dump
module.exports.deleteAndMapFactIndex = deleteAndMapFactIndex
module.exports.deleteAndMapMetadataIndex = deleteAndMapMetadataIndex
module.exports.deleteAndMapUnstructuredPaperIndex = deleteAndMapUnstructuredPaperIndex
module.exports.indexCRMetadata = indexCRMetadata
module.exports.indexEuPMCMetadata = indexEuPMCMetadata
module.exports.loadCRHTMLFullTexts = loadCRHTMLFullTexts
module.exports.loadCRXHTMLFullTexts = loadCRXHTMLFullTexts
module.exports.loadCRPDFFullTexts = loadCRPDFFullTexts
module.exports.loadCRFullTexts = loadCRFullTexts
