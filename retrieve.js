import fs from 'fs'
import execSync from 'child_process'

var getPapers = function (searchstr, dailyset, storedir) {
  // TODO update this to call the latest version of getpapers, and to call it with the query
  // for whichever sources we want URLs for. IF POSSIBLE, have getpapers return the result URLs
  // directly instead of having to read them from disk
  var sd = storedir + '/' + dailyset
  console.log('running getpapers for query ' + searchstr)
  var api = 'eupmc' // should be crossref, and maybe also eupmc if desirable
  var cmd = "getpapers --query '" + searchstr + "' -x --outdir '" + sd + "'"
  var child = execSync(cmd)
  var urls = geturls(sd, api)
  return urls
}

var geturls = function (sd, api) {
  var fln = sd + '/' + api + '_results.json'
  console.log('Reading urls from ' + fln)
  var urls = []
  var jsn = JSON.parse(fs.readFileSync(fln, 'utf8'))
  for (var i in jsn) {
    var ob = jsn[i]
    var url = false
    // look for full text urls, prefer non-subscription, then fall back to DOI or PMCID
    for (var u in ob.fullTextUrlList) {
      var first = true
      for (var n in ob.fullTextUrlList[u].fullTextUrl) {
        if (ob.fullTextUrlList[u].fullTextUrl[n].availability !== 'Subscription required' && url === false) {
          url = ob.fullTextUrlList[u].fullTextUrl[n].url[0]
        } else if (first === true) {
          first = ob.fullTextUrlList[u].fullTextUrl[n].url[0]
        }
      }
      if (url === false && first !== true) {
        url = first
      }
    }
    if (url === false && ob.DOI) {
      url = 'http://dx.doi.org/' + ob.DOI[0]
    }
    if (url === false && ob.pmcid) {
      url = 'http://europepmc.org/articles/PMC' + ob.pmcid[0].replace('PMC', '')
    }
    if (url !== false) urls.push(url)
  }
  console.log('Retrieved ' + urls.length + ' urls')
  return urls
}

module.exports.getpapers = getPapers
