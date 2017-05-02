var util = require( 'util' );
var _ = require( 'lodash' );

exports = module.exports = {};

exports.IllegalArgumentError = function IllegalArgumentError ( message ) {
	Error.captureStackTrace( this, this.constructor );
	this.name = this.constructor.name;
	this.message = message;
};







_.each( exports, function ( error ) {
	util.inherits( error, Error );
});