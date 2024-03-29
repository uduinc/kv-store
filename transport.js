var util = require( 'util' );
var EventEmitter = require( 'events' ).EventEmitter;

var _ = require( 'lodash' );

function Transport ( opts ) {
	opts = opts || {};
	this.priority = opts.priority || Transport.PRIORITY_MEDIUM;
	Object.defineProperty( this, 'empty', { get: function ( ) { return !this.__numWaiting } } );
	Object.defineProperty( this, 'ready', {
		get: function ( ) { return !!this.__ready },
		set: function ( isReady ) {
			if ( isReady && !this.__ready ) {
				this.__ready = true;
				this.emit( 'ready' );
			}
		}
	});
	this.__numWaiting = 0;
}

util.inherits( Transport, EventEmitter );

Transport.PRIORITY_HIGHEST = 1;
Transport.PRIORITY_HIGH = 2;
Transport.PRIORITY_MEDIUM = 3;
Transport.PRIORITY_LOW = 4;
Transport.PRIORITY_LOWEST = 5;

Transport.prototype.__set = function ( k, v, opts, cb ) {
	var self = this;

	if ( !self.ready ) {
		return;
	}
	self.__numWaiting++;
	self.set( k, v, opts, function ( err ) {
		self.__numWaiting--;
		if ( self.empty ) {
			self.emit( 'empty' );
		}
		if ( err ) {
			self.emit( 'error', err );
		}
		if ( cb ) {
			cb( err );
		}
	});
};

Transport.prototype.__update = function ( k, update, opts, cb ) {
	var self = this;

	if ( !self.ready ) {
		return;
	}
	self.__numWaiting++;
	self.update( k, update, opts, function ( err ) {
		self.__numWaiting--;
		if ( self.empty ) {
			self.emit( 'empty' );
		}
		if ( err ) {
			self.emit( 'error', err );
		}
		if ( cb ) {
			cb( err );
		}
	});
};

Transport.prototype.__deleteBy = function ( meta, cb ) {
	var self = this;

	if ( !self.ready ) {
		return cb( 'Transport not ready.' );
	}
	if ( typeof self.deleteBy === 'function' ) {
		self.__numWaiting++;
		self.deleteBy( meta, function ( err ) {
			self.__numWaiting--;
			if ( self.empty ) {
				self.emit( 'empty' );
			}
			if ( err ) {
				self.emit( 'error', err );
			}
			cb( err );
		});
	} else {
		cb( );
	}
};

Transport.prototype.__has = function ( k, cb ) {
	var self = this;

	if ( !self.ready ) {
		return cb( 'Transport not ready.' );
	}
	self.has( k, function ( err, has ) {
		if ( err ) {
			self.emit( 'error', err );
		}
		cb( err, has );
	});
};

Transport.prototype.__get = function ( k, cb ) {
	var self = this;

	if ( !self.ready ) {
		return cb( 'Transport not ready.' );
	}
	self.get( k, function ( err, value, key, meta ) {
		if ( err ) {
			self.emit( 'error', err );
		}
		if ( typeof value !== 'undefined' && !key ) {
			key = k;
		}
		if ( typeof value !== 'undefined' && !meta ) {
			meta = {};
		}
		cb( err, value, key, meta );
	});
};

Transport.prototype.__getAll = function ( keys, cb ) {
	var self = this;

	if ( !self.ready ) {
		return cb( 'Transport not ready.' );
	}
	if ( typeof self.getAll === 'function' ) {
		self.getAll( keys, function ( err, values ) {
			if ( err ) {
				self.emit( 'error', err );
			}
			cb( err, values );
		});
	} else {
		cb( null, {} );
	}
};

Transport.prototype.__findBy = function ( meta, cb ) {
	var self = this;

	if ( !self.ready ) {
		return cb( 'Transport not ready.' );
	}
	if ( typeof self.findBy === 'function' ) {
		self.findBy( meta, function ( err, values ) {
			if ( err ) {
				self.emit( 'error', err );
			}
			cb( err, values );
		});
	} else {
		cb( null, [] );
	}
};

// Transport.prototype.__find = function ( search, cb ) {
// 	if ( !this.ready ) {
// 		return cb( 'Transport not ready.' );
// 	}
// 	if ( typeof this.find === 'function' ) {
// 		this.find( search, cb );
// 	} else {
// 		cb( null, {} );
// 	}
// };

// Transport.prototype.__findOne = function ( search, cb ) {
// 	if ( !this.ready ) {
// 		return cb( 'Transport not ready.' );
// 	}
// 	if ( typeof this.findOne === 'function' ) {
// 		this.findOne( search, cb );
// 	} else {
// 		this.__find( search, function ( err, data ) {
// 			if ( _.isEmpty( data ) ) {
// 				cb( err );
// 			} else {
// 				var firstKey = _.keys( data )[ 0 ];
// 				cb( err, data[ firstKey ], firstKey );
// 			}
// 		});
// 	}
// };

// Transport.prototype.__findByKey = function ( search, cb ) {
// 	if ( typeof search === 'string' ) {
// 		search = new RegExp( search );
// 	} else if ( !( search instanceof RegExp ) ) {
// 		return cb( 'findByKey requires a string or regex' );
// 	}

// 	if ( typeof this.findByKey === 'function' ) {
// 		this.findByKey( search, cb );
// 	} else {
// 		cb( null, {} );
// 	}
// };

// Transport.prototype.__findOneByKey = function ( search, cb ) {
// 	if ( typeof search === 'string' ) {
// 		search = new RegExp( search );
// 	} else if ( !( search instanceof RegExp ) ) {
// 		return cb( 'findOneByKey requires a string or regex' );
// 	}

// 	if ( typeof this.findOneByKey === 'function' ) {
// 		this.findOneByKey( search, cb );
// 	} else {
// 		this.__findByKey( search, function ( err, data ) {
// 			if ( _.isEmpty( data ) ) {
// 				cb( err );
// 			} else {
// 				var firstKey = _.keys( data )[ 0 ];
// 				cb( err, data[ firstKey ], firstKey );
// 			}
// 		});
// 	}
// };

Transport.prototype.__delete = function ( k, cb ) {
	if ( !this.ready ) {
		return;
	}
	var self = this;
	self.__numWaiting++;
	self.delete( k, function ( err ) {
		self.__numWaiting--;
		if ( self.empty ) {
			self.emit( 'empty' );
		}
		if ( err ) {
			self.emit( 'error', err );
		}
		if ( cb ) {
			cb( err );
		}
	});
};



exports = module.exports = Transport;