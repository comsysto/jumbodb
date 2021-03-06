define(['angular' ], function (angular) {
	'use strict';

	/* Controllers */

	return angular.module('jumbodb.controllers', [])
		// Sample controller where service is being used
		.controller('OverviewCtrl', ['$scope', '$http', function ($scope, $http) {
			$http.get('jumbodb/rest/status').success(function(data) {
				$scope.status = data;
			});
		}])
		.controller("HelpCtrl", [function(){}])
		// More involved example where controller is required from an external file
		.controller('CollectionsListCtrl', ["$scope", "$http", function($scope, $http){
			fetchData();
			$scope.msg = {}

			$scope.activateChunkedVersionInCollection = function(chunkDeliveryKey, version, collection) {
				if(confirm('Are you sure?')) {
					$http.put('jumbodb/rest/version/' + chunkDeliveryKey + '/' + version + '/' + collection).success(function(data) {
						fetchData();
						buildMessage(data)
					});
				}
			}

			$scope.deleteChunkedVersionInCollection = function(chunkDeliveryKey, version, collection) {
				if(confirm('Are you sure?')) {
					$http.delete('jumbodb/rest/version/' + chunkDeliveryKey + '/' + version + '/' + collection).success(function(data) {
						fetchData();
						buildMessage(data);
					});
				}
			}

			$scope.deleteCompleteCollection = function(collection) {
				if(confirm('Are you sure?')) {
					$http.delete('jumbodb/rest/collection/' + collection).success(function(data) {
						fetchData();
						buildMessage(data)
					});
				}
			}

			function buildMessage(data) {
				var msg = {};
				msg.error = (data.type == 'delete')
				msg.success = (data.type == 'activate')
				msg.message = data.message;
				$scope.msg = msg;
			}

			function fetchData() {
				$http.get('jumbodb/rest/collections').success(function(data) {
					$scope.collections = data;
				});
			}
		}])
		.controller('DeliveriesListCtrl', ["$scope", "$http", function($scope, $http){
			fetchData();
			$scope.msg = {};

			$scope.showReplication = false;
			$scope.replication = {"port" :12001, "activateChunk": true, "activateVersion": true};

			$scope.toggleReplication = function() {
				$scope.showReplication = !$scope.showReplication;
			}

			$scope.startReplication = function(replication, delivery) {
				replication.deliveryChunkKey = delivery.chunkKey;
				replication.version = delivery.version;
				$http.post('jumbodb/rest/replication', replication).success(function(data) {
					buildMessage(data)
					$scope.showReplication = false;
				});
			}

            $scope.activateChunk = function(chunkDeliveryKey) {
                if(confirm('Are you sure?')) {
                    $http.post('jumbodb/rest/chunk/' + chunkDeliveryKey + '/activate').success(function(data) {
                        fetchData();
                        buildMessage(data)
                    });
                }
            }

            $scope.inactivateChunk = function(chunkDeliveryKey) {
                if(confirm('Are you sure?')) {
                    $http.post('jumbodb/rest/chunk/' + chunkDeliveryKey + '/inactivate').success(function(data) {
                        fetchData();
                        buildMessage(data)
                    });
                }
            }

			$scope.activateChunkedVersion = function(chunkDeliveryKey, version) {
				if(confirm('Are you sure?')) {
					$http.post('jumbodb/rest/version/' + chunkDeliveryKey + '/' + version + '/activate').success(function(data) {
						fetchData();
						buildMessage(data)
					});
				}
			}


			$scope.deleteChunkedVersion = function(chunkDeliveryKey, version) {
				if(confirm('This action is not revertable and will delete all collection within this delivery. Are you sure?')) {
					$http.delete('jumbodb/rest/version/' + chunkDeliveryKey + '/' + version).success(function(data) {
						fetchData();
						buildMessage(data);
					});
				}
			}


			function buildMessage(data) {
				var msg = {};
				msg.error = (data.type == 'delete')
				msg.success = (data.type == 'activate' || data.type == 'success')
				msg.message = data.message;
				$scope.msg = msg;
			}

			function fetchData() {
				$http.get('jumbodb/rest/deliveries').success(function(data) {
					$scope.deliveries = data;
				});
			}
		}])
		.controller('MaintenanceCtrl', ['$scope', '$http', function($scope, $http){
            $scope.info = {};
            $scope.msg = {};
            fetchData();

            $scope.cleanupTemporaryFiles = function() {
                $http.delete('jumbodb/rest/maintenance/tmp/cleanup').success(function(data) {
                    fetchData();
                    buildMessage(data);
                });
            }

            $scope.triggerReloadDatabases = function() {
                $http.post('jumbodb/rest/maintenance/databases/reload').success(function(data) {
                    fetchData();
                    buildMessage(data);
                });
            }


            function buildMessage(data) {
                var msg = {};
                msg.error = (data.type == 'error')
                msg.success = (data.type == 'success')
                msg.message = data.message;
                $scope.msg = msg;
            }

            function fetchData() {
                $http.get('jumbodb/rest/maintenance/tmp/info').success(function(data) {
                    $scope.info = data;
                });
            }
        }])
		.controller('HelpCtrl', [function(){}])
		// More involved example where controller is required from an external file
		.controller('BrowseCtrl', ['$scope', '$http', function($scope, $http) {
			$scope.results = {};
			$scope.collections = [];
			$scope.query = "{\"limit\": 5}";

			$http.get('jumbodb/rest/query/collections').success(function(data) {
				$scope.collections = data;
			});

			$scope.setCurrentCollection = function() {
				var res = $scope.collections;
				for(var i = 0; i < res.length; i++) {
					if(res[i].collection == $scope.collection) {
						$scope.currentCollection = res[i];
						break;
					}
				}
			}

			$scope.search = function(collection, query) {
				$http.post('jumbodb/rest/query/' + collection + "/defaultLimit", query).success(function(data) {
					$scope.results = data;
					buildMessage(data);
				});
			}

			$scope.formatJson = function(json) {
				delete json["$$hashKey"]
				var jsonStr = JSON.stringify(json, undefined, 3);
				return syntaxHighlight(jsonStr);
			}

			function buildMessage(data) {
				var msg = {};
				if(data.message) {
					msg.error = true;
					msg.message = data.message;
					$scope.msg = msg;
				}
			}

			function syntaxHighlight(json) {
				json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
				return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
					var cls = 'number';
					if (/^"/.test(match)) {
						if (/:$/.test(match)) {
							cls = 'key';
						} else {
							cls = 'string';
						}
					} else if (/true|false/.test(match)) {
						cls = 'boolean';
					} else if (/null/.test(match)) {
						cls = 'null';
					}
					return '<span class="' + cls + '">' + match + '</span>';
				});
			}
		}])
		.controller("ReplicationCtrl",["$scope", "$http", "$timeout", function($scope, $http, $timeout) {
			fetchData();

			$scope.abortReplication = function(id) {
				if(confirm('Are you sure?')) {
					$http.put('jumbodb/rest/replication/' + id).success(function(data) {
						fetchData();
						buildMessage(data);
					});
				}
			}

			$scope.deleteReplication = function(id) {
				if(confirm('Are you sure?')) {
					$http.delete('jumbodb/rest/replication/' + id).success(function(data) {
						fetchData();
						buildMessage(data);
					});
				}
			}

			function buildMessage(data) {
				var msg = {};
				msg.error = (data.type == 'delete')
				msg.message = data.message;
				$scope.msg = msg;
			}

			$scope.fetch = function() {
				$timeout(function() {
					fetchData();
					$scope.fetch();
				}, 1000);
			}
			$scope.fetch();
			function fetchData() {
				$http.get('jumbodb/rest/replication').success(function(data) {
					$scope.replications = data;
				});
			}
		}])
		.controller("MonitoringCtrl", ["$scope", "$http", function($scope, $http){

			$scope.tabs = [
				{content: "Server Information under construction..", title: "Server", active: true},
				{content: "Query information under construction..", title: "Query", active: false},
				{content: "Import information under construction..", title: "Import", active: false}
			];
		}]);
});
