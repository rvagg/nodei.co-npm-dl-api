const level    = require('level')
    , sublevel = require('level-sublevel')
    , moment   = require('moment')


module.exports.db        = sublevel(level('./counts.db'))
module.exports.packageDb = module.exports.db.sublevel('package')
module.exports.countDb   = module.exports.db.sublevel('count')
module.exports.sumDb     = module.exports.db.sublevel('sum')

module.exports.packageCountDb = function (pkg) {
  return module.exports.countDb.sublevel('_' + pkg)
}

module.exports.dateSumDb = function (date) {
  return module.exports.sumDb.sublevel(date)
}

module.exports.packageDateDb = function (date) {
  return module.exports.packageDb.sublevel(date)
}

