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
			})
			socket.on("disconnect", function(){
				//getCollection(function(collection){
					delete _self;			
				//});
			});
		};
		
	dsInstance.prototype = {
		set : function(data, callback){
			var _self = this;
			this.itemCreatePromise.then(function(res){
				_self.collection.update({_id : _self._id}, data, callback);
			});
		},
		send : function(event, params, data, callback) {
			var eventRules = rules[event] || {},
				_self = this;
			this.itemCreatePromise.then(function(err, res){
				_self.collection.findOne({_id : _self._id}, function(err, currentSocketData){
					console.log("------------------------------");
					console.log(currentSocketData);
					var currentSocketData = currentSocketData || {};
					var query  = JSON.parse(eventRules.replace(/\"?__(me|param)\.(\w+)\"?/g, function(str, source, prop){
							var sourceObj = source == "me" ? currentSocketData : params;
							if(sourceObj[prop]) {
								return JSON.stringify(sourceObj[prop]);
							}
							else {
								throw new Error(str + " is not found");
							}
						}).replace(/\"?__(\w+)\((.+?)\)\"?/g, function(str, getter, args){
							if(typeof getters[getter] == "function") {
								return JSON.stringify(getters[getter].apply(getters, (args && args.split(","))));
							} else {
								throw new Error(getter+" getter is not function or not defined");
							}							
						}));					
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
				rules[key] = JSON.stringify(dataRules[key]);
			}
		},
		getInstance : function(socket, _id){
			return new dsInstance(socket, _id);
		},
		setupGetter : function(name, getter) {
			getters[name] = getter;
			return this;
		}
	}
}