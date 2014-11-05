const through2     = require('through2')
    , log          = require('bole')('process-packages')
    , listPackages = require('./npm-list-packages')
    , db           = require('./db')


function updatePackageList (callback) {
  var packages = listPackages()

  function savePackage (pkg, _, callback) {
    db.packageDb.get(pkg, function (err) {
      if (err && err.notFound) {
        db.packageDb.put(pkg, '{}', function (err) {
          callback(err)
        })
      } else {
        callback(err)
      }
    })
  }

  function onErrorOrEnd (err) {
    if (err)
      log.error(err)
    else
      log.debug('Completed package list update from npm')

    callback && callback(err)
    callback = null
  }

  log.debug('Updating package list from npm')

  packages
    .on('error', onErrorOrEnd)
    .pipe(through2.obj(savePackage))
    .on('error', onErrorOrEnd)
    .on('finish', onErrorOrEnd)
}


module.exports = updatePackageList
