var _ = require( 'lodash' );
var sha1 = require( 'crypto-js' ).SHA1;

exports = module.exports = {};

exports.hash = function ( input ) {
	if ( typeof input !== 'string' ) {
		input = JSON.stringify( input );
	}

	return sha1( input ).toString( );
};

exports.dataSize = function ( input ) {
	return JSON.stringify( input ).length*2;
};