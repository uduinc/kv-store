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
		console.error( 'Error in KVStore ->', err.stack );
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
		console.error( 'Error in KVStore[', name, '] ->', err );
	});
	var priorityIndex = _.findIndex( this.priorityList, function ( t ) {
		return t.transport.priority > transport.priority;
	});
	if ( priorityIndex === -1 ) {
		this.priorityList.push( { name: name, transport: transport } );
	} else {
		this.priorityList.splice( priorityIndex, 0, transport );
	}
};

KVStore.prototype.set = function ( k, v, opts, cb ) {
	var self = this;

	if ( typeof opts === 'function' ) {
		cb = opts;
		opts = {};
	} else if ( !opts ) {
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

	_.each( self.transports, function ( transport, name ) {
		if ( opts.transports && !~opts.transports.indexOf( name ) ) {
			return callback( );
		}
		if ( opts.transportExclusions && ~opts.transportExclusions.indexOf( name ) ) {
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
	} else if ( !opts ) {
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

	_.each( self.transports, function ( transport, name ) {
		if ( opts.transports && !~opts.transports.indexOf( name ) ) {
			return callback( );
		}
		if ( opts.transportExclusions && ~opts.transportExclusions.indexOf( name ) ) {
			return callback( );
		}
		transport.__delete( k, callback );
	});
};

KVStore.prototype.deleteBy = function ( search, opts, cb ) {
	var self = this;

	if ( typeof opts === 'function' ) {
		cb = opts;
		opts = {};
	} else if ( !opts ) {
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

	_.each( self.transports, function ( transport, name ) {
		if ( opts.transports && !~opts.transports.indexOf( name ) ) {
			return callback( );
		}
		if ( opts.transportExclusions && ~opts.transportExclusions.indexOf( name ) ) {
			return callback( );
		}
		transport.__deleteBy( search, callback );
	});
};

KVStore.prototype.deleteByMeta = function ( meta, opts, cb ) {
	this.deleteBy( { meta: meta }, opts, cb );
};

KVStore.prototype.get = function ( k, opts, cb ) {
	var self = this;

	if ( typeof opts === 'function' ) {
		cb = opts;
		opts = {};
	} else if ( !opts ) {
		opts = {};
	}

	if ( typeof k !== 'string' ) {
		k = 'k_' + utils.hash( k );
	}

	if ( _.isNumber( opts.retry ) && opts.retry > 0 ) {
		var actualCb = cb;
		cb = function ( err, value, key, meta ) {
			if ( err || !value ) {
				setTimeout( function ( self, k, opts, cb ) {
					self.get( k, _.merge( opts, { retry: opts.retry - 1 } ), cb );
				}, opts.retryDelay || 1000, self, k, opts, actualCb );
			} else {
				actualCb( err, value, key, meta );
			}
		};
	}

	var pullers = [];
	( function getNext( i ) {
		if ( i >= self.priorityList.length ) {
			return cb( );
		}

		var next = self.priorityList[ i ];
		if ( opts.transports && !~opts.transports.indexOf( next.name ) ) {
			return getNext( i+1 );
		}
		if ( opts.transportExclusions && ~opts.transportExclusions.indexOf( next.name ) ) {
			return getNext( i+1 );
		}
		next.transport.__get( k, function ( err, value, key, meta ) {
			if ( err ) {
				self.emit( 'error', err );
			}
			if ( value ) {
				_.each( pullers, function ( t ) {
					t.__set( key, value, { meta: meta, pulled: true } );
				});
				cb( err, value, key, meta );
			} else {
				if ( next.transport.pull ) {
					pullers.push( next.transport );
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
	} else if ( !opts ) {
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

		var next = self.priorityList[ i ];
		if ( opts.transports && !~opts.transports.indexOf( next.name ) ) {
			return getNext( i+1 );
		}
		if ( opts.transportExclusions && ~opts.transportExclusions.indexOf( next.name ) ) {
			return getNext( i+1 );
		}
		next.transport.__getAll( keys, function ( err, v ) {
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
	} else if ( !opts ) {
		opts = {};
	}

	( function findNext( i ) {
		if ( i < 0 ) {
			return cb( null, [] );
		}

		var next = self.priorityList[ i ];
		if ( opts.transports && !~opts.transports.indexOf( next.name ) ) {
			return findNext( i-1 );
		}
		if ( opts.transportExclusions && ~opts.transportExclusions.indexOf( next.name ) ) {
			return findNext( i-1 );
		}
		next.transport.__findByMeta( meta, function ( err, v ) {
			if ( err ) {
				self.emit( 'error', err );
			}
			if ( v && v.length ) {
				cb( null, v );
			} else {
				findNext( i-1 );
			}
		});
	})( self.priorityList.length-1 );
};

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