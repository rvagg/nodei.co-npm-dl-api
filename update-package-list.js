const through2     = require('through2')
    , listPackages = require('./npm-list-packages')
    , db           = require('./db')


function updatePackageList (callback) {
  var packages = listPackages()

  function savePackage (pkg, _, callback) {
    db.packageDb.get(pkg, function (err, value) {
      if (err && err.notFound) {
        db.packageDb.put(pkg, '{}', { keyEncoding: 'utf8' }, function (err) {
          callback(err)
        })
      } else {
        callback(err)
      }
    })
  }

  function onErrorOrEnd (err) {
    callback && callback(err)
    callback = null
  }

  packages
    .on('error', onErrorOrEnd)
    .pipe(through2.obj(savePackage))
    .on('error', onErrorOrEnd)
    .on('finish', onErrorOrEnd)
}


module.exports = updatePackageList
