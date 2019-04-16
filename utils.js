var bson = new ( require( 'bson' ) )( );
var objhash = require( 'node-object-hash' )( { coerce: false } ).hash;
var uuid = require( 'uuid/v4' );

exports = module.exports = {};

exports.hash = function ( input ) {
	try {
		return objhash( input );
	} catch ( err ) {
		// fall back on uuid
		return uuid( );
	}
	return objhash( input );
};

exports.dataSize = function ( input ) {
	// return JSON.stringify( input ).length*2;
	if ( input === null || input === undefined ) {
		return bson.calculateObjectSize( {} );
	}
	return bson.calculateObjectSize( input );
};