'use strict'

var jsonist      = require('jsonist')
  , map          = require('map-async')
  , userPackages = require('../nodei.co-pkginfo-api/user-packages')

  , users        = process.argv.slice(2)


map(users, userPackages, processUsers)


function processUsers (err, userPackages) {
  var packages

  if (err)
    throw err

  packages = userPackages.reduce(function r (p, c) { return p.concat(c) }, [])
  packages.sort()
  packages = packages.filter(function f (p, i) { return packages[i + 1] != p })

  map(packages, downloads, processDownloads)
}


function downloads (pkg, callback) {
  jsonist.get('https://nodei.co/api/npm-dl/download-sum/' + pkg, callback)
}


function processDownloads (err, packageDownloads) {
  var packagesTotal

  if (err)
    throw err

  packagesTotal = packageDownloads
                    .filter(isFinite)
                    .reduce(function r (p, c) { return p + c }, 0)

  jsonist.get('https://nodei.co/api/npm-dl/total', function afterGet (err, total) {
    if (err)
      throw err

    console.log('total=' + total.total + ', packagesTotal=' + packagesTotal + ', percent=' + (Math.round((packagesTotal / total.total) * 10000) / 100))
  })
}

