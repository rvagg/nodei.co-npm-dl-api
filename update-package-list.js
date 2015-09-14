'use strict'

const log          = require('bole')('process-packages')
    , listPackages = require('./npm-list-packages')
    , db           = require('./db')


function updatePackageList (callback) {
  function savePackage (pkg, callback) {
    db.packageDb.get(pkg, (err) => {
      if (err && err.notFound)
        return db.packageDb.put(pkg, '{}', callback)
      callback(err)
    })
  }

  log.debug('Updating package list from npm')

  listPackages((err, names) => {
    if (err)
      log.error(err)
    else
      log.debug('Completed package list update from npm')

    let i = 0

    function save (err) {
      if (err)
        throw new Error(`Error saving package ${err.message}`)

      if (++i == names.length)
        return callback()

      savePackage(names[i], save)
    }

    save()
  })
}


module.exports = updatePackageList

if (require.main === module)
  updatePackageList(() => {})
