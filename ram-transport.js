var util = require( 'util' );

var _ = require( 'lodash' );

var utils = require( './utils' );
var Transport = require( './transport' );


function RamTransport ( opts ) {
	opts = opts || {};
	opts.priority = opts.priority || Transport.PRIORITY_HIGHEST;
	Transport.call( this, opts );

	this.storage = {};
	this.maxDataSize = opts.maxDataSize || 10000000;
	this.ttl = opts.ttl || 120000;
	this.pull = opts.hasOwnProperty( 'pull' ) ? !!opts.pull : true;
	this.ready = true;
}

util.inherits( RamTransport, Transport );

RamTransport.prototype.set = function ( k, v, opts, cb ) {
	if ( utils.dataSize( v ) < this.maxDataSize ) {
		this.unsetTimer( k );
		this.storage[ k ] = { value: v, meta: opts.meta || {} };
		this.setTimer( k );
	}
	if ( cb ) {
		process.nextTick( cb );
	}
};

RamTransport.prototype.get = function ( k, cb ) {
	var self = this;
	if ( self.storage.hasOwnProperty( k ) ) {
		self.unsetTimer( k );
		self.setTimer( k );
		process.nextTick( function ( ) {
			// console.log( 'RAM>', self.storage[ k ].value );
			// console.log( 'RAM> FOUND' );
			cb( null, self.storage[ k ].value, k, _.cloneDeep( self.storage[ k ].meta ) );
		});
	} else {
		process.nextTick( function ( ) {
			// console.log( 'RAM> <EMPTY>' );
			cb( null );
		});
	}
};

RamTransport.prototype.has = function ( k, cb ) {
	var self = this;
	if ( self.storage.hasOwnProperty( k ) ) {
		self.unsetTimer( k );
		self.setTimer( k );
		process.nextTick( function ( ) {
			cb( null, true );
		});
	} else {
		process.nextTick( function ( ) {
			cb( null, false );
		});
	}
};

RamTransport.prototype.getAll = function ( keys, cb ) {
	var self = this;
	var values = {};
	_.each( keys, function ( k ) {
		if ( self.storage[ k ] ) {
			self.unsetTimer( k );
			self.setTimer( k );
			values[ k ] = _.cloneDeep( self.storage[ k ].value );
		}
	});
	process.nextTick( function ( ) {
		cb( null, values );
	});
};

RamTransport.prototype.delete = function ( k, cb ) {
	// console.log( '!!! deleting', k );
	this.unsetTimer( k );
	delete this.storage[ k ];
	if ( cb ) {
		process.nextTick( cb );
	}
};

RamTransport.prototype.deleteBy = function ( search, cb ) {
	var self = this;
	var keys = _.keys( self.storage );
	var numKeys = keys.length;

	var hasComparator = function ( obj ) {
		return obj.hasOwnProperty( '$neq' )
			|| obj.hasOwnProperty( '$gt' )
			|| obj.hasOwnProperty( '$lt' )
			|| obj.hasOwnProperty( '$gte' )
			|| obj.hasOwnProperty( '$lte' )
			|| obj.hasOwnProperty( '$in' )
			|| obj.hasOwnProperty( '$nin' );
	}

	var compare = function ( a, b ) {
		if ( !a || typeof a !== 'object' || !b || typeof b !== 'object' ) return a === b;
		var bKeys = _.keys( b );

		var matches = true;
		for ( var i=0; i<bKeys.length && matches; i++ ) {
			var k = bKeys[ i ];
			if ( b[ k ] && typeof b[ k ] === 'object' && hasComparator( b[ k ] ) ) {
				if ( b[ k ].$neq ) {
					matches = matches && a[ k ] !== b[ k ].$neq;
				}
				if ( b[ k ].$gt ) {
					matches = matches && a[ k ] > b[ k ].$gt;
				}
				if ( b[ k ].$lt ) {
					matches = matches && a[ k ] < b[ k ].$lt;
				}
				if ( b[ k ].$gte ) {
					matches = matches && a[ k ] >= b[ k ].$gte;
				}
				if ( b[ k ].$lte ) {
					matches = matches && a[ k ] <= b[ k ].$lte;
				}
				if ( b[ k ].$in && Array.isArray( b[ k ].$in ) ) {
					matches = matches && b[ k ].$in.indexOf( a[ k ] ) !== -1;
				}
				if ( b[ k ].$nin && Array.isArray( b[ k ].$nin ) ) {
					matches = matches && b[ k ].$nin.indexOf( a[ k ] ) === -1;
				}
			} else {
				matches = compare( a[ k ], b[ k ] );
			}
		}
		return matches;
	};

	( function deleteByInternal ( idx ) {
		var stop = idx + 100;
		for ( var i=idx; i<stop && i<numKeys; i++ ) {
			var key = keys[ i ];
			if ( self.storage[ key ] && compare( self.storage[ key ], search ) ) {
				self.delete( key );
			}
		}
		if ( i < numKeys ) {
			setImmediate( function ( ) {
				deleteByInternal( stop );
			});
		} else if ( cb ) {
			cb( );
		}
	})( 0 );
};

RamTransport.prototype.unsetTimer = function ( k ) {
	if ( this.storage[ k ] ) {
		// console.log( 'Clearing timer for', k );
		clearTimeout( this.storage[ k ].timer );
	}
};

RamTransport.prototype.setTimer = function ( k ) {
	if ( this.storage[ k ] ) {
		// console.log( 'Setting timer to clear', k, 'after', this.ttl );
		this.storage[ k ].timer = setTimeout( this.delete.bind( this, k ), this.ttl );
	}
};




exports = module.exports = RamTransport;