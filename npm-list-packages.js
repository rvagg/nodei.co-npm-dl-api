const hyperquest = require('hyperquest')
    , JSONStream = require('JSONStream')

    , ALL_PACKAGES_URL = 'https://registry.npmjs.org/-/all' //?limit=130'


// load the list of all npm libs with 'repo' pointing to GitHub
function listPackages () {
  var stream = JSONStream.parse('*.name')
    , req    = hyperquest(ALL_PACKAGES_URL)

  req.on('error', stream.emit.bind(stream, 'error'))

  return req.pipe(stream)
}


module.exports = listPackages
