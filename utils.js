var _ = require( 'lodash' );
var bson = new ( require( 'bson' ) )( );
var objhash = require( 'node-object-hash' )( { coerce: false } ).hash;

exports = module.exports = {};

exports.hash = function ( input ) {
	return objhash( input );
};

exports.dataSize = function ( input ) {
	// return JSON.stringify( input ).length*2;
	if ( input === null || input === undefined ) {
		return bson.calculateObjectSize( {} );
	}
	return bson.calculateObjectSize( input );
};