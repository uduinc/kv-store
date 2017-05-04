var util = require( 'util' );
var EventEmitter = require( 'events' ).EventEmitter;

var _ = require( 'lodash' );

var errs = require( './errors' );
var IllegalArgumentError = errs.IllegalArgumentError;
var Transport = require( './transport' );
var utils = require( './utils' );

function KVStore ( transports ) {
	var self = this;

	self.transports = {};
	self.priorityList = [];

	self.on( 'error', function ( err ) {
		console.error( 'Error in KVStore ->', err, '-----', err.stack );
	});

	_.each( transports, function ( v, k ) {
		self.addTransport( k, v );
	});
}

util.inherits( KVStore, EventEmitter );

KVStore.prototype.addTransport = function ( name, transport ) {
	if ( !( transport instanceof Transport ) ) {
		throw new IllegalArgumentError( 'Transport must be an instance of Transport.' );
	}

	this.transports[ name ] = transport;
	transport.on( 'error', function ( err ) {
		console.error( 'Error in KVStore[', name, '] ->', err, '-----', err.stack );
	});
	var priorityIndex = _.findIndex( this.priorityList, function ( t ) {
		return t.priority > transport.priority;
	});
	if ( priorityIndex === -1 ) {
		this.priorityList.push( transport );
	} else {
		this.priorityList.splice( priorityIndex, 0, transport );
	}
};

KVStore.prototype.set = function ( k, v, opts, cb ) {
	var self = this;

	if ( typeof opts === 'function' ) {
		cb = opts;
		opts = {};
	}

	var waitingCount = _.size( self.transports );
	var errList = [];
	var callback = function ( err ) {
		if ( cb ) {
			if ( err ) {
				self.emit( 'error', err );
				errList.push( err );
			}
			if ( !( --waitingCount ) ) {
				if ( !errList.length ) {
					errList = null;
				}
				process.nextTick( function ( ) {
					cb( errList );
				});
			}
		}
	};

	if ( k === null ) {
		k = 'v_' + utils.hash( v );
	} else if ( typeof k !== 'string' ) {
		k = 'k_' + utils.hash( k );
	}

	_.each( self.transports, function ( transport ) {
		if ( opts.transports && !~opts.transports.indexOf( transport.name ) ) {
			return callback( );
		}
		if ( opts.transportExclusions && ~opts.transportExclusions.indexOf( transport.name ) ) {
			return callback( );
		}
		transport.__set( k, v, opts, callback );
	});
	return k;
};

KVStore.prototype.delete = function ( k, opts, cb ) {
	var self = this;

	if ( typeof opts === 'function' ) {
		cb = opts;
		opts = {};
	}

	if ( typeof k !== 'string' ) {
		k = 'k_' + utils.hash( k );
	}

	var waitingCount = _.size( self.transports );
	var errList = [];
	var callback = function ( err ) {
		if ( cb ) {
			if ( err ) {
				self.emit( 'error', err );
				errList.push( err );
			}
			if ( !( --waitingCount ) ) {
				if ( !errList.length ) {
					errList = null;
				}
				process.nextTick( function ( ) {
					cb( errList );
				});
			}
		}
	};

	_.each( self.transports, function ( transport ) {
		if ( opts.transports && !~opts.transports.indexOf( transport.name ) ) {
			return callback( );
		}
		if ( opts.transportExclusions && ~opts.transportExclusions.indexOf( transport.name ) ) {
			return callback( );
		}
		transport.__delete( k, callback );
	});
};

KVStore.prototype.deleteByMeta = function ( meta, opts, cb ) {
	var self = this;

	if ( typeof opts === 'function' ) {
		cb = opts;
		opts = {};
	}

	var waitingCount = _.size( self.transports );
	var errList = [];
	var callback = function ( err ) {
		if ( cb ) {
			if ( err ) {
				self.emit( 'error', err );
				errList.push( err );
			}
			if ( !( --waitingCount ) ) {
				if ( !errList.length ) {
					errList = null;
				}
				process.nextTick( function ( ) {
					cb( errList );
				});
			}
		}
	};

	_.each( self.transports, function ( transport ) {
		if ( opts.transports && !~opts.transports.indexOf( transport.name ) ) {
			return callback( );
		}
		if ( opts.transportExclusions && ~opts.transportExclusions.indexOf( transport.name ) ) {
			return callback( );
		}
		transport.__deleteByMeta( meta, callback );
	});
};

KVStore.prototype.get = function ( k, opts, cb ) {
	var self = this;

	if ( typeof opts === 'function' ) {
		cb = opts;
		opts = {};
	}

	if ( typeof k !== 'string' ) {
		k = 'k_' + utils.hash( k );
	}

	var pullers = [];
	( function getNext( i ) {
		if ( i >= self.priorityList.length ) {
			return cb( );
		}

		var transport = self.priorityList[ i ];
		if ( opts.transports && !~opts.transports.indexOf( transport.name ) ) {
			return getNext( i+1 );
		}
		if ( opts.transportExclusions && ~opts.transportExclusions.indexOf( transport.name ) ) {
			return getNext( i+1 );
		}
		transport.__get( k, function ( err, value, key ) {
			if ( err ) {
				self.emit( 'error', err );
			}
			if ( value ) {
				_.each( pullers, function ( t ) {
					t.__set( key, value, { pulled: true } );
				});
				cb( err, value, key );
			} else {
				if ( transport.pull ) {
					pullers.push( transport );
				}
				getNext( i+1 );
			}
		});
	})( 0 );
};

KVStore.prototype.getAll = function ( keys, opts, cb ) {
	var self = this;

	if ( typeof opts === 'function' ) {
		cb = opts;
		opts = {};
	}

	keys = _.map( keys, function ( k ) {
		if ( typeof k !== 'string' ) {
			return 'k_' + utils.hash( k );
		}
		return k;
	});

	var values = {};
	( function getNext( i ) {
		if ( i >= self.priorityList.length ) {
			return cb( null, values );
		}

		var transport = self.priorityList[ i ];
		if ( opts.transports && !~opts.transports.indexOf( transport.name ) ) {
			return getNext( i+1 );
		}
		if ( opts.transportExclusions && ~opts.transportExclusions.indexOf( transport.name ) ) {
			return getNext( i+1 );
		}
		transport.__getAll( keys, function ( err, v ) {
			if ( err ) {
				self.emit( 'error', err );
			}
			if ( v && !_.isEmpty( v ) ) {
				values = _.assign( {}, v, values );
			}
			getNext( i+1 );
		});
	})( 0 );
};

KVStore.prototype.findByMeta = function ( meta, opts, cb ) {
	var self = this;

	if ( typeof opts === 'function' ) {
		cb = opts;
		opts = {};
	}

	var values = {};
	( function findNext( i ) {
		if ( i >= self.priorityList.length ) {
			return cb( null, values );
		}

		var transport = self.priorityList[ i ];
		if ( opts.transports && !~opts.transports.indexOf( transport.name ) ) {
			return findNext( i+1 );
		}
		if ( opts.transportExclusions && ~opts.transportExclusions.indexOf( transport.name ) ) {
			return findNext( i+1 );
		}
		transport.__findByMeta( meta, function ( err, v ) {
			if ( err ) {
				self.emit( 'error', err );
			}
			if ( v && !_.isEmpty( v ) ) {
				values = _.assign( {}, v, values );
			}
			findNext( i+1 );
		});
	})( 0 );
};

// KVStore.prototype.find = function ( search, opts, cb ) {
// 	// returns as soon as it finds any results from a transport, even if a lower-priority transport
// 	// may have more results
// 	var self = this;

// 	if ( typeof opts === 'function' ) {
// 		cb = opts;
// 		opts = {};
// 	}

// 	( function findNext( i ) {
// 		if ( i >= self.priorityList.length ) {
// 			return cb( null, {} );
// 		}

// 		var transport = self.priorityList[ i ];
// 		if ( opts.transports && !~opts.transports.indexOf( transport.name ) ) {
// 			return findNext( i+1 );
// 		}
// 		if ( opts.transportExclusions && ~opts.transportExclusions.indexOf( transport.name ) ) {
// 			return findNext( i+1 );
// 		}
// 		transport.__find( search, function ( err, values ) {
// 			if ( err ) {
// 				self.emit( 'error', err );
// 			}
// 			if ( values && !_.isEmpty( values ) ) {
// 				cb( err, values );
// 			} else {
// 				findNext( i+1 );
// 			}
// 		});
// 	})( 0 );
// };

// KVStore.prototype.findOne = function ( search, opts, cb ) {
// 	var self = this;

// 	if ( typeof opts === 'function' ) {
// 		cb = opts;
// 		opts = {};
// 	}

// 	( function findNext( i ) {
// 		if ( i >= self.priorityList.length ) {
// 			return cb( );
// 		}

// 		var transport = self.priorityList[ i ];
// 		if ( opts.transports && !~opts.transports.indexOf( transport.name ) ) {
// 			return findNext( i+1 );
// 		}
// 		if ( opts.transportExclusions && ~opts.transportExclusions.indexOf( transport.name ) ) {
// 			return findNext( i+1 );
// 		}
// 		transport.__findOne( search, function ( err, value, key ) {
// 			if ( err ) {
// 				self.emit( 'error', err );
// 			}
// 			if ( value ) {
// 				cb( err, value, key );
// 			} else {
// 				findNext( i+1 );
// 			}
// 		});
// 	})( 0 );
// };

// KVStore.prototype.findByKey = function ( search, opts, cb ) {
// 	// returns as soon as it finds any results from a transport, even if a lower-priority transport
// 	// may have more results
// 	var self = this;

// 	if ( typeof opts === 'function' ) {
// 		cb = opts;
// 		opts = {};
// 	}

// 	( function findNext( i ) {
// 		if ( i >= self.priorityList.length ) {
// 			return cb( null, {} );
// 		}

// 		var transport = self.priorityList[ i ];
// 		if ( opts.transports && !~opts.transports.indexOf( transport.name ) ) {
// 			return findNext( i+1 );
// 		}
// 		if ( opts.transportExclusions && ~opts.transportExclusions.indexOf( transport.name ) ) {
// 			return findNext( i+1 );
// 		}
// 		transport.__findByKey( search, function ( err, values ) {
// 			if ( err ) {
// 				self.emit( 'error', err );
// 			}
// 			if ( values && !_.isEmpty( values ) ) {
// 				cb( err, values );
// 			} else {
// 				findNext( i+1 );
// 			}
// 		});
// 	})( 0 );
// };

// KVStore.prototype.findOneByKey = function ( search, opts, cb ) {
// 	var self = this;

// 	if ( typeof opts === 'function' ) {
// 		cb = opts;
// 		opts = {};
// 	}

// 	( function findNext( i ) {
// 		if ( i >= self.priorityList.length ) {
// 			return cb( );
// 		}

// 		var transport = self.priorityList[ i ];
// 		if ( opts.transports && !~opts.transports.indexOf( transport.name ) ) {
// 			return findNext( i+1 );
// 		}
// 		if ( opts.transportExclusions && ~opts.transportExclusions.indexOf( transport.name ) ) {
// 			return findNext( i+1 );
// 		}
// 		transport.__findOneByKey( search, function ( err, value, key ) {
// 			if ( err ) {
// 				self.emit( 'error', err );
// 			}
// 			if ( value ) {
// 				cb( err, value, key );
// 			} else {
// 				findNext( i+1 );
// 			}
// 		});
// 	})( 0 );
// };

KVStore.prototype.ready = function ( ) {
	var transports = _.keys( this.transports );
	for ( var i=0, len=transports.length; i<len; i++ ) {
		if ( !this.transports[ transports[ i ] ].ready ) {
			return false;
		}
	}
	return true;
};

KVStore.Transport = Transport;
KVStore.RamTransport = require( './ram-transport' );
KVStore.MongoTransport = require( './mongo-transport' );

exports = module.exports = KVStore;