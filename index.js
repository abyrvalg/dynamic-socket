module.exports = function(io, dbConfig) {
	var rules  = {},
		getters = {},
		mongoClient = require('mongodb').MongoClient,
		q = require("promise"),
		connect = q.denodeify(function(callback){
			mongoClient.connect('mongodb://'+dbConfig.host+':'+(dbConfig.post || '27017')+'/'+(dbConfig.db || "sockets"), function(err, result){
				callback(err, result && result.collection && result.collection(dbConfig.collection || "sockets"))
			});			
		})(),
		dsInstance = function(socket, _id){
			if(!socket) {throw new Error("socket parameter is not defined")};
			if(!_id) {throw new Error("_id parameter is not defined")};
			this.socketID = socket.id;
			this._id = _id || socket.id;
			var _self = this;
			connect.then(function(collection) {
				_self.collection = collection;
				_self.itemCreatePromise = q.denodeify(_self.collection.update).call(_self.collection, {_id : _self._id}, {$set : {socketID : _self.socketID}}, {upsert : true});
			});			
			socket.on("disconnect", function(){
				_self.itemCreatePromise.then(function(){
					_self.collection.update({_id : _self.id}, {$unset : {socketID : 1}}, function(err){
						if(!err) {
							delete _self;
						}
					})
				})			
			});
		};
		
	dsInstance.prototype = {
		set : function(data, callback){
			var _self = this;
			this.itemCreatePromise.then(function(res){
				_self.collection.update({_id : _self._id}, data, callback);
			});
		},
		get : function(data, callback) {
			var _self = this;
			this.itemCreatePromise.then(function(){
				_self.collection.findOne({_id: _self._id}, data, callback);
			})
		},
		send : function(event, params, data, callback) {
			var eventRules = rules[event] || {},
				_self = this;
			this.itemCreatePromise.then(function(err, res){
				_self.collection.findOne({_id : _self._id}, function(err, sender){
					var currentSocketData = currentSocketData || {};
					var query  = typeof eventRules == "function" ? eventRules(sender, params) : eventRules;
					query.socketID = {$ne : _self.socketID};
					_self.collection.find(query, function(err, result){						
						result.each(function(err, record){
							record && io.sockets.socket(record.socketID).emit(event, data);
						})
					});
				});				
			});
		}
	};
	return {
		setupRules : function(dataRules){
			for(key in dataRules) {
				rules[key] = dataRules[key];
			}
		},
		getInstance : function(socket, _id){
			return new dsInstance(socket, _id);
		}
	}
}