'use strict'

var updaterLog       = require('bole')('updater')
  , updatePackages   = require('./update-package-list')
  , allDownloads     = require('./npm-all-downloads')
  , periodicInterval = 1000 * 60 * 60 * 12

// TODO: avoid overlap of periodic jobs
setInterval(periodic, periodicInterval)
setTimeout(periodic, 1000)// * 60)


function periodic () {
  var start = Date.now()

  updaterLog.info('Starting periodic update')

  updatePackages(function afterUpdate (err) {
    if (err) {
      updaterLog.error(err)
      return process.exit(1)
    }

    allDownloads.processAllPackages(function afterProcess (err) {
      if (err) {
        updaterLog.error(err)
        return process.exit(1)
      }

      updaterLog.info('Finished periodic update, took ' + (Date.now() - start) + ' seconds')
    })
  })
}
