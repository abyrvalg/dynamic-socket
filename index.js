module.exports = function(io, dbConfig) {
	var rules  = {},
		getters = {},
		mongoClient = require('mongodb').MongoClient,	
		getCollection = (function(){
			var collection;
			return function (callback) {
				if(collection) { 
					callback(collection) 
				} else {
					mongoClient.connect('mongodb://'+dbConfig.host+':'+(dbConfig.post || '27017')+'/'+(dbConfig.db || "sockets"), function(err, res){
						collection = res.collection(dbConfig.collection || "sockets");
						callback(collection);
					});
				}
			}
		})(),
		dsInstance = function(socket, _id){
			this.socketID = socket.id
			this._id = _id || socket.id;
			var _self = this;
			socket.on("disconnect", function(){
				getCollection(function(collection){
					collection.remove({socketID : _self.socketID}, function(err){
						!err && (delete _self);
					});					
				});
			});
		};
		
	dsInstance.prototype = {
		set : function(data, callback){
			var _self = this;
			getCollection(function(collection){
				data["$set"] = data["$set"] || {}
				data["$set"].socketID = _self.socketID;
				collection.update({_id : _self._id}, data, {upsert : true}, callback || (function(err, result){
					console.log(arguments)
				}))
			});			
		},
		send : function(event, params, data, callback) {
			var eventRules = rules[event] || {},
				_self = this;
				
			getCollection(function(collection){
				collection.findOne({socketID : _self.socketID}, function(err, currentSocketData){					
					var currentSocketData = currentSocketData || {},
						query  = JSON.parse(eventRules.replace(/\"?__(me|param)\.(\w+)\"?/g, function(str, source, prop){
							var sourceObj = source == "me" ? currentSocketData : params;
							console.log(sourceObj);
							if(sourceObj[prop]) {
								return sourceObj[prop];
							}
							else {
								throw new Error(str + " is not found");
							}
						}).replace(/\"?__(\w+)\((.+?)\)\"?/g, function(str, getter, args){
							if(typeof getters[getter] == "function") {
								var result = getters[getter].apply(getters, (args && args.split(",")))
								return typeof result == "string" ? '"'+reuslt+'"' : result;
							} else {
								throw new Error(getter+" getter is not function or not defined");
							}							
						}));					
					query.socketID = {$ne : _self.socketID};
					collection.find(query, function(err, result){						
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
		getInstane : function(socket, _id){
			return new dsInstance(socket, _id);
		},
		setupGetter : function(name, getter) {
			getters[name] = getter;
		}
	}
}