'use strict'

var hyperquest = require('hyperquest')
  , path       = require('path')
  , bl         = require('bl')
  , fs         = require('fs')


var allPackagesUrl = 'https://registry.npmjs.org/-/all/static/all.json?limit=1000'
  , debugSource    = path.join(__dirname, '/all.json')


// load the list of all npm libs with 'repo' pointing to GitHub
function listPackages (callback) {
  function source () {
    if (process.env.DEBUG && fs.statSync(debugSource))
      return fs.createReadStream(debugSource)
    return hyperquest(allPackagesUrl)
  }

  source().pipe(bl(function afterPipe (err, data) {
    var packages, names

    if (err)
      return callback(err)

    try {
      packages = JSON.parse(data.toString())
    } catch (e) {
      return  callback(e)
    }

    names = Object.keys(packages).filter(function f (name) {
      var versions = packages[name].versions

      return versions && Object.keys(versions).length > 0
    })

    callback(null, names)
  }))
}


module.exports = listPackages


if (require.main === module) {
  listPackages(function afterList (err, names) {
    if (err)
      throw err
    console.log('' + names.length + ' packages')
  })
}
