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
			$scope.explain = {};
			$scope.collections = [];
			$scope.sqlQuery = "";
			$scope.jsonQuery = "";
			$scope.explainedSql = {};
            $scope.loading = false;

			$http.get('jumbodb/rest/query/collections').success(function(data) {
				$scope.collections = data;
			});

			$scope.setCurrentCollectionJson = function() {
				var res = $scope.collections;
				for(var i = 0; i < res.length; i++) {
					if(res[i].collection == $scope.collectionJson) {
						$scope.currentCollection = res[i];
                        $scope.jsonQuery = '{"collection": "' + $scope.currentCollection.collection + '", "limit": 5}';
					}
				}
			}

			$scope.setCurrentCollectionSql = function() {
				var res = $scope.collections;
				for(var i = 0; i < res.length; i++) {
					if(res[i].collection == $scope.collectionSql) {
                        $scope.currentCollection = res[i];
                        $scope.sqlQuery = 'SELECT * FROM ' + $scope.currentCollection.collection + ' LIMIT 5';
					}
				}
			}

			$scope.searchJsonQuery = function(jsonQuery) {
                $scope.loading = true;
                $scope.results = {};
                $http.post('jumbodb/rest/query/json/defaultLimit', jsonQuery).success(function(data) {
					$scope.results = data;
                    $scope.loading = false;
					buildMessage(data);
                });
			}

            $scope.searchSqlQuery = function(sqlQuery) {
                $scope.loading = true;
                $scope.results = {};
                $http.post('jumbodb/rest/query/sql/defaultLimit', sqlQuery).success(function(data) {
                    $scope.results = data;
                    $scope.loading = false;
                    buildMessage(data);
                });
            }

            $scope.explainSqlQuery = function(sqlQuery) {
                $scope.loading = true;
                $scope.results = {};
                $http.post('jumbodb/rest/query/sql/explain', sqlQuery).success(function(data) {
                    $scope.explainedSql = data;
                    $scope.loading = false;
                    buildMessage(data);
                });
            }

			$scope.formatJson = function(elem, json) {
				delete json["$$hashKey"];
                $(elem).JSONView(json);
//                console.log(elem);
//				var jsonStr = JSON.stringify(json, undefined, 3);
//				return syntaxHighlight(jsonStr);
			}

			function buildMessage(data) {
                $scope.msg = {};
				if(data.message) {
                    $scope.msg.error = true;
                    $scope.msg.message = data.message;
				}
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
