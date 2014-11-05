const hyperquest = require('hyperquest')
    , JSONStream = require('JSONStream')


const ALL_PACKAGES_URL = 'https://registry.npmjs.org/-/all' //?limit=1000'


// load the list of all npm libs with 'repo' pointing to GitHub
function listPackages () {
  var stream = JSONStream.parse('*.name')

  hyperquest(ALL_PACKAGES_URL)
    .on('error', stream.emit.bind(stream, 'error'))
    .pipe(stream)

  return stream
}


module.exports = listPackages