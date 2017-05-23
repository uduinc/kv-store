var util = require( 'util' );
var EventEmitter = require( 'events' ).EventEmitter;

var _ = require( 'lodash' );
var mongo = require( 'mongodb' ).MongoClient;
var bson = new ( require( 'bson' ) ).BSONPure.BSON( );

var utils = require( './utils' );
var errs = require( './errors' );
var IllegalArgumentError = errs.IllegalArgumentError;
var Transport = require( './transport' );


var DOT_SEPARATOR_REPLACEMENT = '|_DOT_SEPARATOR_|';

function MongoTransport ( opts ) {
	opts = opts || {};
	opts.dependencyInterval = opts.dependencyInterval || 30000 || 3600000;
	Transport.call( this, opts );

	var self = this;

	self.maxObjectSize = opts.maxObjectSize || 14000000;
	self.pieceLength = Math.min( self.maxObjectSize, opts.pieceSize || 10000000 ) / 2;
	self.dependencyGracePeriod = opts.dependencyGracePeriod || 5000;
	self.dependencyCheckPrecondition = opts.dependencyCheckPrecondition || function ( cb ) { cb( true ); };
	self.dependencyCheckCallback = opts.dependencyCheckCallback;

	if ( opts.collection ) {
		self.collectionName = opts.collection;
	} else {
		throw new IllegalArgumentError( 'MongoTransport requires a collection' );
	}

	if ( opts.db ) {
		self.db = opts.db;
		self.collection = self.db.collection( self.collectionName );
		self.ready = true;
		self.dependencyInterval = setInterval( self.checkDependencies.bind( self ), opts.dependencyInterval );
	} else if ( opts.connection_string ) {
		mongo.connect( opts.connection_string, function ( err, db ) {
			if ( err ) {
				throw err;
			}

			self.db = db;
			self.collection = db.collection( self.collectionName );

			self.collection.ensureIndex( 'key', { unique: true }, function ( err ) {
				if ( err ) {
					throw err;
				}

				self.collection.ensureIndex( 'expiration', { expireAfterSeconds: 0, sparse: true }, function ( err ) {
					if ( err ) {
						throw err;
					}

					self.collection.ensureIndex( 'dependencies', { sparse: true }, function ( err ) {
						if ( err ) {
							throw err;
						}

						self.ready = true;
						self.dependencyInterval = setInterval( self.checkDependencies.bind( self ), opts.dependencyInterval );
					});
				});
			});
		});
	} else {
		throw new IllegalArgumentError( 'MongoTransport requires either an existing db connection or a connection string' );
	}
}

util.inherits( MongoTransport, Transport );

// helper functions
MongoTransport.prototype.checkOneDependent = function ( dependencies, cb ) {
	var self = this;
	var depRemaining = _.keys( dependencies ).length;
	var allowed = true;
	var abort = false;

	var searches = {};


	_.each( dependencies, function ( dv, dk ) {
		if ( abort || !allowed ) return false;

		if ( dk === 'KV_KEEP_ALIVE' && dv ) {
			cb( null, true );
			abort = true;
			return false;
		}

		var spl = dk.split( DOT_SEPARATOR_REPLACEMENT );
		var collection = self.db.collection( spl.shift( ) );

		var search = {};
		search[ spl.join( '.' ) ] = dv;

		console.log( 'Checking dependency:', search );

		collection.count( search, function ( err, count ) {
			if ( err ) {
				console.error( 'Error checking dependency existence in MongoTransport:', err );
				abort = true;
				cb( err );
			} else {
				// console.log( 'Got count:', count );
				if ( allowed && !abort ) {
					if ( !count ) {
						allowed = false;
						cb( null, false );
					} else if ( !( --depRemaining ) ) {
						cb( null, true );
					}
				}
			}
		});
	});
};

var flatten = function ( obj, prefix ) {
	if ( !obj || typeof obj !== 'object' || Array.isArray( obj ) || obj instanceof RegExp || obj instanceof Date ) {
		return obj;
	}
	prefix = prefix ? prefix + '.' : '';
	var newObj = {};
	_.each( obj, function ( v, k ) {
		k = k.replace( /\./g, DOT_SEPARATOR_REPLACEMENT );
		if ( v && typeof v === 'object' && !( Array.isArray( v ) || v instanceof RegExp || v instanceof Date ) ) {
			_.merge( newObj, flatten( v, prefix + k ) )
		} else {
			newObj[ prefix + k ] = v;
		}
	});
	return newObj;
};

var escapeDots = function ( obj ) {
	if ( !obj || typeof obj !== 'object' || Array.isArray( obj ) || obj instanceof RegExp || obj instanceof Date ) {
		return obj;
	}
	var newObj = {};
	_.each( obj, function ( v, k ) {
		newObj[ k.replace( /\./g, DOT_SEPARATOR_REPLACEMENT ) ] = escapeDots( obj[ k ] );
	});
	return newObj;
};

var unescapeDots = function ( obj ) {
	if ( !obj || typeof obj !== 'object' || Array.isArray( obj ) || obj instanceof RegExp || obj instanceof Date ) {
		return obj;
	}
	return _.transform( obj, function ( result, v, k ) {
		if ( v && typeof v === 'object' && !( Array.isArray( v ) || v instanceof RegExp || v instanceof Date ) ) {
			result[ k.split( DOT_SEPARATOR_REPLACEMENT ).join( '.' ) ] = unescapeDots( v );
		} else {
			result[ k.split( DOT_SEPARATOR_REPLACEMENT ).join( '.' ) ] = v;
		}
	});
};


MongoTransport.prototype.set = function ( k, v, opts, cb ) {
	var self = this;
	var obj = { key: k, value: escapeDots( v ) };
	if ( opts.expiration ) {
		obj.expiration = new Date( opts.expiration );
	} else if ( opts.ttl ) {
		obj.expiration = new Date( Date.now( ) + opts.ttl );
	}
	obj.meta = opts.meta || {};
	var metaSize = bson.calculateObjectSize( obj.meta );
	if ( metaSize > self.maxMetaSize ) {
		var err = new Error( 'Could not insert; meta obj over size limit.' );
		err.meta = {
			key: k,
			metaSize: metaSize,
			maxMetaSize: self.maxMetaSize
		};
		return cb( err );
	}

	var setInternal = function ( ) {
		var updateObj = {
			$set: _.omit( obj, 'dependencies', 'expiration' )
		};
		if ( obj.expiration ) {
			updateObj[ '$max' ] = { expiration: obj.expiration };
		} else {
			updateObj[ '$unset' ] = { expiration: true };
		}
		if ( obj.dependencies ) {
			updateObj[ '$addToSet' ] = { dependencies: obj.dependencies };
		}

		// NOTE: this will break for anything larger than ~695,000 times pieceSize (= 2*pieceLength).
		// Not an issue for the forseeable future, but something to keep in mind.
		if ( bson.calculateObjectSize( obj ) > self.maxObjectSize ) {
			// TODO: do this without relying on JSON.stringify
			var str = JSON.stringify( obj.value );
			var keys = [];

			// UnorderedBulkOp has better performance, but would break if multiple pieces are identical since
			// upsert is not atomic; Ordered also allows us to include the primary doc in the batch
			var batch = self.collection.initializeOrderedBulkOp( );
			for ( var idx = 0; idx < str.length; idx += self.pieceLength ) {
				var piece = str.slice( idx, idx+self.pieceLength );
				var pieceObj = _.assign( {}, updateObj, {
					key: utils.hash( piece ),
					value: piece,
					meta: obj.meta
				});
				if ( obj.dependencies ) {
					pieceObj.dependencies = obj.dependencies;
				}
				if ( obj.expiration ) {
					pieceObj.expiration = obj.expiration;
				}
				keys.push( pieceObj.key );
				batch.find( { key: pieceObj.key } ).upsert( ).updateOne( pieceObj );
			}
			obj.value = keys;
			obj.piece_split = true;
			batch.find( { key: k } ).upsert( ).updateOne( obj );
			batch.execute( cb );
		} else {
			self.collection.update( { key: k }, updateObj, { upsert: true }, cb );
		}
	};


	// NOTE: Dependencies are an AND requirement -- ALL dependencies must be met for the document to remain
	if ( opts.dependencies && _.size( opts.dependencies ) ) {
		obj.dependencies = _.mapKeys( opts.dependencies, function ( v, k ) {
			return k.replace( /\./g, DOT_SEPARATOR_REPLACEMENT );
		});

		var dependencyChecker = new MongoDependencyCheck( self.db, self.dependencyGracePeriod );
		dependencyChecker.on( 'error', self.emit.bind( self, 'error' ) );

		var gotCB = false;
		dependencyChecker.addDependencyCheck( obj.dependencies, function ( allowed ) {
			if ( gotCB ) return;
			gotCB = true;
			if ( allowed ) {
				setInternal( );
			} else {
				var err = new Error( 'Could not insert; dependency not met.' );
				err.meta = {
					key: k,
					dependencies: opts.dependencies
				};
				return cb( err );
			}
		});

		dependencyChecker.on( 'destroy', function ( ) {
			if ( gotCB ) return;
			gotCB = true;
			var err = new Error( 'Error checking dependencies when inserting.' );
			err.meta = {
				key: k,
				dependencies: opts.dependencies
			};
			return cb( err );
		});

		dependencyChecker.run( );
	} else {
		obj.dependencies = { 'KV_KEEP_ALIVE': true };
		setInternal( );
	}
};

MongoTransport.prototype.getPieces = function ( pieces, cb, key, meta ) {
	self.collection.find( { key: { $in: pieces } }, function ( err, cursor ) {
		if ( err ) {
			// console.log( 'DB > ERR' );
			return cb( err );
		}
		var piecesFound = 0;
		var abort = false;
		cursor.each( function ( err, piece ) {
			if ( abort ) return;
			if ( err ) {
				abort = true;
				// console.log( 'DB > ERR' );
				return cb( err );
			}
			if ( !piece ) {
				if ( piecesFound === pieces.length ) {
					// console.log( 'DB > FOUND' );
					return cb( null, unescapeDots( JSON.parse( pieces.join( '' ) ) ), key, meta );
				} else {
					var err = new Error( 'Incomplete data found.' );
					err.key = k;
					// console.log( 'DB > ERR' );
					return cb( err );
				}
			}

			( function next( startIdx ) {
				var idx = pieces.indexOf( piece.key, startIdx );
				if ( ~idx ) {
					pieces[ idx ] = piece.value;
					piecesFound++;
					next( idx+1 );
				}
			})( 0 );
		});
	});
};

MongoTransport.prototype.get = function ( k, cb ) {
	var self = this;
	self.collection.findOne( { key: k }, function ( err, data ) {
		if ( err || !data ) {
			// console.log( 'DB >', err ? 'ERR' : '<EMPTY>' );
			cb( err );
		} else {
			if ( data.piece_split ) {
				self.getPieces( data.value, cb, k, data.meta );
			} else {
				// console.log( 'DB >', data.value );
				// console.log( 'DB > FOUND' );
				cb( err, unescapeDots( data.value ), k, data.meta );
			}
		}
	});
};

MongoTransport.prototype.getAll = function ( keys, cb ) {
	var self = this;
	self.collection.find( { key: { $in: keys } } ).toArray( function ( err, data ) {
		if ( err || !data.length) {
			return cb( err, {} );
		}

		var returnData = {};
		var numWaiting = 1;
		var abort = false;

		var checkDone = function ( ) {
			if ( abort ) return;
			if ( !( --numWaiting ) ) {
				cb( null, returnData );
			}
		}
		_.each( data, function ( d ) {
			if ( abort ) return false;
			if ( d.piece_split ) {
				numWaiting++;
				self.getPieces( d.value, function ( err, val ) {
					if ( err ) {
						abort = true;
						return cb( err );
					}
					returnData[ d.key ] = val;
					checkDone( );
				});
			} else {
				returnData[ d.key ] = d.value;
			}
		});
		checkDone( );
	});
};

MongoTransport.prototype.findByMeta = function ( search, cb ) {
	this.collection.find( flatten( search, 'meta' ), { key: 1, value: 1 } ).toArray( function ( err, data ) {
		if ( err ) {
			cb( err );
		} else if ( data && data.length ) {
			var dataObj = {};
			_.each( data, function ( d ) {
				dataObj[ d.key ] = d.value;
			});
			cb( null, dataObj );
		} else {
			cb( null, {} );
		}
	});
};

MongoTransport.prototype.delete = function ( k, cb ) {
	var callback = function ( err ) {
		if ( cb ) {
			cb( err );
		}
	};

	var self = this;

	self.collection.findOne( { key: k }, { piece_split: 1 }, function ( err, data ) {
		if ( err ) {
			if ( cb ) {
				cb( err );
			}
			return;
		}
		if ( data && data.piece_split ) {
			var pieces = data.value.concat( k );
			self.collection.remove( { key: { $in: data.value.concat( k ) } }, callback );
		} else {
			self.collection.remove( { key: k }, callback );
		}
	});
};

MongoTransport.prototype.deleteBy = function ( search, cb ) {
	this.collection.remove( flatten( search ), cb );
};

MongoTransport.prototype.checkDependencies = function ( cb ) {
	var self = this;
	self.dependencyCheckPrecondition( function ( runCheck ) {
		if ( !runCheck ) return;

		// console.log( '~~~ checking dependencies' );
		var cursor = self.collection.find( { dependencies: { $exists: true, $ne: [] } }, { key: 1, dependencies: 1, _id: 0 } );
		var abort = false;
		var hadAny = false;
		var hadFailures = false;
		var remaining = 0;
		var batch = self.collection.initializeUnorderedBulkOp( );
		var keysToRemove = [];

		var checker = new MongoDependencyCheck( self.db, self.dependencyGracePeriod );
		checker.on( 'error', self.emit.bind( self, 'error' ) );
		checker.on( 'destroy', function ( ) {
			if ( self.dependencyCheckCallback ) {
				self.dependencyCheckCallback( );
			}
		});

		var checkDone = function ( ) {
			if ( !( --remaining ) && hadFailures ) {
				if ( keysToRemove.length ) {
					batch.find( { key: { $in: keysToRemove } } ).remove( );
				}
				batch.execute( function ( err ) {
					if ( err ) {
						console.error( 'Error executing batch when clearing expired dependencies in MongoTransport:', err );
						self.emit( 'error', err );
					}
					// console.log( '~~~ executed dependency batch' );
				});
			}
		};
		cursor.each( function ( err, d ) {
			if ( err ) {
				return self.emit( 'error', err );
			}
			if ( !d ) {
				if ( hadAny ) {
					checker.run( );
				}
				return;
			}

			hadAny = true;
			remaining++;
			var innerDependencyCounter = d.dependencies.length;
			var toPull = [];
			// console.log( 'Checking dependencies on', d.key );
			_.each( d.dependencies, function ( dependencies ) {
				checker.addDependencyCheck( dependencies, function ( allowed ) {
					if ( !allowed ) {
						console.log( 'Failed dependencies on', d.key, '->', dependencies );
						toPull.push( dependencies );
						hadFailures = true;
					} else {
						console.log( 'Succeeded dependencies on', d.key, '->', dependencies );
					}
					if ( !( --innerDependencyCounter ) ) {
						if ( toPull.length === d.dependencies.length ) {
							// All dependencies have failed
							console.log( 'All dependencies failed, removing', d.key );
							keysToRemove.push( d.key );
						} else if ( toPull.length ) {
							console.log( d.key, '=>', { $pullAll: { dependencies: toPull } } );
							batch.find( { key: d.key } ).updateOne( { $pullAll: { dependencies: toPull } } );
						}
						checkDone( );
					}
				});
			});
		});
	});
};








function MongoDependencyCheck ( db, gracePeriod ) {
	this.toCheck = {};
	this.db = db;
	if ( gracePeriod ) {
		this.gracePeriod = gracePeriod;
	}
}

util.inherits( MongoDependencyCheck, EventEmitter );

MongoDependencyCheck.prototype.addDependencyCheck = function ( dependencies, cb ) {
	if ( this.running ) return;
	var totalDeps = _.size( dependencies );
	if ( !totalDeps ) {
		return cb( true );
	}


	var self = this;
	var abort = false;
	var callback = function ( allowed ) {
		if ( abort ) return;
		if ( !allowed ) {
			abort = true;
			cb( false );
		} else if ( !( --totalDeps ) ) {
			cb( true );
		}
	};
	_.each( dependencies, function ( v, k ) {
		if ( k === 'KV_KEEP_ALIVE' && v ) {
			cb( true );
			abort = true;
			return false;
		}
		var spl = k.split( DOT_SEPARATOR_REPLACEMENT );
		var collection = spl.shift( );
		var searchKey = spl.join( '.' );
		if ( !self.toCheck[ collection ] ) {
			self.toCheck[ collection ] = {};
			self.toCheck[ collection ][ searchKey ] = {};
		} else if ( !self.toCheck[ collection ][ searchKey ] ) {
			self.toCheck[ collection ][ searchKey ] = {};
		}
		if ( !self.toCheck[ collection ][ searchKey ][ v ] ) {
			self.toCheck[ collection ][ searchKey ][ v ] = [ callback ];
		} else {
			self.toCheck[ collection ][ searchKey ][ v ].push( callback );
		}
	});
};

MongoDependencyCheck.prototype.run = function ( ) {
	if ( this.running ) return;
	console.log( '>>> running dependency check' );
	console.log( JSON.stringify( this.toCheck, null, 4 ) );
	this.running = true;
	var self = this;
	var collectionsRemaining = _.size( self.toCheck );
	if ( !collectionsRemaining ) return;

	var abort = false;
	var cbsRemaining = 0;
	_.each( self.toCheck, function ( searchKeys, collection ) {
		if ( abort ) return;
		var search = [];
		var filter = { _id: 0 };
		_.each( searchKeys, function ( values, key ) {
			var searchObj = {};
			searchObj[ key ] = { $in: _.keys( values ) };
			cbsRemaining += _.size( values );
			search.push( searchObj );
			filter[ key ] = 1;
		});
		console.log( '>>> running check on', collection, 'for:', util.inspect( { $or: search }, { depth: null } ) );
		self.db.collection( collection ).find( { $or: search }, filter, function ( err, cursor ) {
			if ( abort ) return;
			if ( err ) {
				abort = true;
				self.emit( 'error', err );
				self.destroy( );
				return;
			}

			cursor.each( function ( err, doc ) {
				if ( abort ) return;
				if ( err ) {
					abort = true;
					self.emit( 'error', err );
					self.destroy( );
					return;
				}
				if ( !doc ) {
					if ( !( --collectionsRemaining ) ) {
						self.finish( cbsRemaining );
					}
					return;
				}

				( function processObj ( value, prefix ) {
					if ( typeof value !== 'object' || Array.isArray( value ) || value instanceof RegExp || value instanceof Date ) {
						if ( self.toCheck[ collection ][ prefix ] && self.toCheck[ collection ][ prefix ][ value ] ) {
							_.each( self.toCheck[ collection ][ prefix ][ value ], function ( fn ) {
								fn( true );
							});
							cbsRemaining--;
							delete self.toCheck[ collection ][ prefix ][ value ];
						}
					} else {
						_.each( value, function ( v, k ) {
							processObj( v, prefix ? prefix + '.' + k : k, self.toCheck[ collection ] );
						});
					}
				})( doc, '' );
			});
		});
	});
};

MongoDependencyCheck.prototype.finish = function ( failedDependencies ) {
	var self = this;
	if ( failedDependencies && self.gracePeriod ) {
		console.log( '>>> Waiting', self.gracePeriod, 'ms then re-running under grace period' );
		setTimeout( function ( ) {
			self.gracePeriod = 0;
			self.running = false;
			self.run( );
		}, self.gracePeriod );
	} else {
		_.each( self.toCheck, function ( searchKeys, collection ) {
			_.each( searchKeys, function ( values, key ) {
				_.each( values, function ( cbs ) {
					_.each( cbs, function ( cb ) {
						cb( false );
					});
				});
			});
		});
		self.destroy( );
	}
};

MongoDependencyCheck.prototype.destroy = function ( ) {
	console.log( '>>> destroying MongoDependencyCheck' );
	this.emit( 'destroy' );
	this.toCheck = null;
	this.db = null;
	this.removeAllListeners( );
};














exports = module.exports = MongoTransport;