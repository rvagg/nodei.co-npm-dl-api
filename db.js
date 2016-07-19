var level          = require('level')
  , NpmDownloadsDb = require('npm-download-db')

module.exports = new NpmDownloadsDb(level('./counts.db'))
